using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using Twinbox.Transport;
using SnsAttribute = Amazon.SimpleNotificationService.Model.MessageAttributeValue;
using SqsAttribute = Amazon.SQS.Model.MessageAttributeValue;

namespace Twinbox.AmazonSqs;

internal static class AmazonSqsMapping
{
    public const string ContentTypeAttribute = "content-type";
    public const string BodyEncodingAttribute = "twinbox-body-encoding";
    public const string HeadersAttribute = "twinbox-headers";
    public const string ErrorHeader = "twinbox-error";

    /// <summary>Queue a dead-lettered copy came from.</summary>
    public const string OriginHeader = "twinbox-origin";

    public const string Base64Encoding = "base64";

    /// <summary>SQS rejects messages with more message attributes than this.</summary>
    public const int MaxAttributes = 10;

    /// <summary>FIFO messages without a partition key share one group, so they stay in send order.</summary>
    public const string DefaultGroupId = "twinbox";

    public const string ReceiveCountAttribute = "ApproximateReceiveCount";
    public const string GroupIdAttribute = "MessageGroupId";

    private const string FallbackContentType = "application/octet-stream";
    private const int MaxFifoIdLength = 128;
    private const int MaxAttributeNameLength = 256;
    private const int MaxErrorLength = 2000;

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    // Given their own attribute first when headers overflow, so subscription filters and tracing can still see them.
    private static readonly string[] PreferredHeaders = [TransportHeaders.TraceParent, TransportHeaders.TenantId];

    public static bool IsFifo(string queueOrTopic) => queueOrTopic.EndsWith(".fifo", StringComparison.Ordinal);

    public static OutgoingMessage ToOutgoing(TransportMessage message, bool fifo)
    {
        ArgumentNullException.ThrowIfNull(message);
        var body = EncodeBody(message.Body.Span, message.ContentType, out var base64);
        var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
        AddIfPresent(attributes, TransportHeaders.MessageId, message.MessageId);
        AddIfPresent(attributes, TransportHeaders.MessageName, message.MessageName);
        AddIfPresent(attributes, ContentTypeAttribute, message.ContentType);
        AddIfPresent(attributes, TransportHeaders.PartitionKey, message.PartitionKey);
        if (base64)
        {
            attributes[BodyEncodingAttribute] = Base64Encoding;
        }

        AddHeaders(attributes, message.Headers);
        return new OutgoingMessage(
            body,
            attributes,
            fifo ? FifoId(message.PartitionKey) ?? DefaultGroupId : null,
            fifo ? FifoId(message.MessageId) : null);
    }

    public static SendMessageRequest ToSendRequest(OutgoingMessage message, string queueUrl) => new()
    {
        QueueUrl = queueUrl,
        MessageBody = message.Body,
        MessageAttributes = message.Attributes.ToDictionary(
            a => a.Key,
            a => new SqsAttribute { DataType = "String", StringValue = a.Value },
            StringComparer.Ordinal),
        MessageGroupId = message.GroupId,
        MessageDeduplicationId = message.DeduplicationId,
    };

    public static Amazon.SimpleNotificationService.Model.PublishRequest ToPublishRequest(OutgoingMessage message, string topicArn) => new()
    {
        TopicArn = topicArn,
        Message = message.Body,
        MessageAttributes = message.Attributes.ToDictionary(
            a => a.Key,
            a => new SnsAttribute { DataType = "String", StringValue = a.Value },
            StringComparer.Ordinal),
        MessageGroupId = message.GroupId,
        MessageDeduplicationId = message.DeduplicationId,
    };

    public static IncomingMessage ToIncoming(Message message, string source)
    {
        ArgumentNullException.ThrowIfNull(message);
        var attributes = ReadAttributes(message.MessageAttributes);
        var body = message.Body ?? string.Empty;
        var fallbackId = message.MessageId;
        if (!attributes.ContainsKey(TransportHeaders.MessageId) && TryUnwrapNotification(body, out var notification))
        {
            (body, attributes, fallbackId) = notification;
        }

        var headers = ExpandHeaders(attributes);
        var systemAttributes = message.Attributes;
        string? groupId = null;
        systemAttributes?.TryGetValue(GroupIdAttribute, out groupId);

        return new IncomingMessage(
            NullIfEmpty(headers.GetValueOrDefault(TransportHeaders.MessageId)) ?? fallbackId ?? string.Empty,
            headers.GetValueOrDefault(TransportHeaders.MessageName) ?? string.Empty,
            source,
            DecodeBody(body, attributes.GetValueOrDefault(BodyEncodingAttribute)),
            NullIfEmpty(headers.GetValueOrDefault(ContentTypeAttribute)) ?? FallbackContentType,
            headers,
            DeliveryAttempt(systemAttributes),
            NullIfEmpty(headers.GetValueOrDefault(TransportHeaders.PartitionKey)) ?? NullIfEmpty(groupId));
    }

    /// <summary>Re-sends what was received with the failure attached; the message id is kept so the copy keeps its identity.</summary>
    public static TransportMessage ToDeadLetter(IncomingMessage message, Exception error, string deadLetterQueue)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(error);
        var headers = new Dictionary<string, string>(message.Headers, StringComparer.Ordinal);
        var description = $"{error.GetType().Name}: {error.Message}";
        headers[ErrorHeader] = description.Length <= MaxErrorLength ? description : description[..MaxErrorLength];
        headers[OriginHeader] = message.Source;
        return new TransportMessage(
            message.MessageId, message.MessageName, deadLetterQueue, message.Body, message.ContentType, headers, message.PartitionKey);
    }

    /// <summary>Ids SQS would reject (too long, spaces, non-ASCII) are replaced by a stable hash so FIFO still works.</summary>
    public static string? FifoId(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        if (value.Length <= MaxFifoIdLength && value.All(c => c is >= '!' and <= '~'))
        {
            return value;
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    }

    public static bool IsValidAttributeName(string name)
    {
        if (name.Length is 0 or > MaxAttributeNameLength
            || name[0] == '.'
            || name[^1] == '.'
            || name.Contains("..", StringComparison.Ordinal)
            || name.StartsWith("AWS.", StringComparison.OrdinalIgnoreCase)
            || name.StartsWith("Amazon.", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return name.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.');
    }

    /// <summary>SQS accepts only the XML character range in bodies and attribute values.</summary>
    public static bool IsValidText(string value)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (char.IsHighSurrogate(c) && i + 1 < value.Length && char.IsLowSurrogate(value[i + 1]))
            {
                i++;
                continue;
            }

            if (!(c is '\t' or '\n' or '\r' || (c >= ' ' && c <= '퟿') || (c >= '' && c <= '�')))
            {
                return false;
            }
        }

        return true;
    }

    public static int DeliveryAttempt(Dictionary<string, string>? systemAttributes) =>
        systemAttributes?.TryGetValue(ReceiveCountAttribute, out var raw) == true
        && int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var count)
        && count > 0
            ? count
            : 1;

    private static string EncodeBody(ReadOnlySpan<byte> body, string contentType, out bool base64)
    {
        if (IsTextual(contentType))
        {
            try
            {
                var text = StrictUtf8.GetString(body);
                if (IsValidText(text))
                {
                    base64 = false;
                    return text;
                }
            }
            catch (DecoderFallbackException)
            {
                // Not really UTF-8 despite the content type; fall through to base64.
            }
        }

        base64 = true;
        return Convert.ToBase64String(body);
    }

    private static byte[] DecodeBody(string body, string? encoding)
    {
        if (string.Equals(encoding, Base64Encoding, StringComparison.OrdinalIgnoreCase))
        {
            var buffer = new byte[body.Length * 3 / 4];
            if (Convert.TryFromBase64String(body, buffer, out var written))
            {
                return buffer[..written];
            }
        }

        // A corrupt base64 body reaches the handler as-is and fails deserialization, which dead-letters it intact.
        return Encoding.UTF8.GetBytes(body);
    }

    private static bool IsTextual(string contentType)
    {
        var mediaType = contentType.Split(';', 2)[0].Trim();
        return mediaType.StartsWith("text/", StringComparison.OrdinalIgnoreCase)
            || mediaType.Equals("application/json", StringComparison.OrdinalIgnoreCase)
            || mediaType.Equals("application/xml", StringComparison.OrdinalIgnoreCase)
            || mediaType.EndsWith("+json", StringComparison.OrdinalIgnoreCase)
            || mediaType.EndsWith("+xml", StringComparison.OrdinalIgnoreCase);
    }

    private static void AddIfPresent(Dictionary<string, string> attributes, string name, string? value)
    {
        if (!string.IsNullOrEmpty(value) && IsValidText(value))
        {
            attributes[name] = value;
        }
    }

    private static void AddHeaders(Dictionary<string, string> attributes, IReadOnlyDictionary<string, string> headers)
    {
        var eligible = new List<KeyValuePair<string, string>>();
        var overflow = new List<KeyValuePair<string, string>>();
        foreach (var header in headers)
        {
            if (attributes.ContainsKey(header.Key) || header.Key is HeadersAttribute or BodyEncodingAttribute)
            {
                continue;
            }

            (IsValidAttributeName(header.Key) && header.Value.Length > 0 && IsValidText(header.Value) ? eligible : overflow).Add(header);
        }

        eligible.Sort((a, b) => Rank(a.Key) != Rank(b.Key) ? Rank(a.Key).CompareTo(Rank(b.Key)) : string.CompareOrdinal(a.Key, b.Key));
        var free = MaxAttributes - attributes.Count;
        var individual = overflow.Count == 0 && eligible.Count <= free ? eligible.Count : Math.Max(free - 1, 0);
        foreach (var header in eligible.Take(individual))
        {
            attributes[header.Key] = header.Value;
        }

        overflow.AddRange(eligible.Skip(individual));
        if (overflow.Count > 0)
        {
            attributes[HeadersAttribute] = WriteJson(overflow);
        }

        static int Rank(string name) => Array.IndexOf(PreferredHeaders, name) is var index and >= 0 ? index : PreferredHeaders.Length;
    }

    private static Dictionary<string, string> ExpandHeaders(Dictionary<string, string> attributes)
    {
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        if (attributes.TryGetValue(HeadersAttribute, out var json))
        {
            ReadJson(json, headers);
        }

        foreach (var (name, value) in attributes)
        {
            if (name is not (HeadersAttribute or BodyEncodingAttribute))
            {
                headers[name] = value;
            }
        }

        return headers;
    }

    private static Dictionary<string, string> ReadAttributes(Dictionary<string, SqsAttribute>? attributes)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in attributes ?? [])
        {
            if (value.StringValue is { } text)
            {
                result[name] = text;
            }
        }

        return result;
    }

    /// <summary>Recognises an SNS envelope, which arrives when a subscription was made without raw message delivery.</summary>
    private static bool TryUnwrapNotification(string body, out (string Body, Dictionary<string, string> Attributes, string? MessageId) notification)
    {
        notification = default;
        if (!body.StartsWith('{'))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(body);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("Type", out var type) || type.ValueKind != JsonValueKind.String || type.GetString() != "Notification"
                || !root.TryGetProperty("TopicArn", out _)
                || !root.TryGetProperty("Message", out var message) || message.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
            if (root.TryGetProperty("MessageAttributes", out var messageAttributes) && messageAttributes.ValueKind == JsonValueKind.Object)
            {
                foreach (var attribute in messageAttributes.EnumerateObject())
                {
                    if (attribute.Value.ValueKind == JsonValueKind.Object
                        && attribute.Value.TryGetProperty("Type", out var kind) && kind.GetString() is "String" or "Number"
                        && attribute.Value.TryGetProperty("Value", out var value) && value.ValueKind == JsonValueKind.String)
                    {
                        attributes[attribute.Name] = value.GetString()!;
                    }
                }
            }

            var id = root.TryGetProperty("MessageId", out var messageId) && messageId.ValueKind == JsonValueKind.String ? messageId.GetString() : null;
            notification = (message.GetString()!, attributes, id);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string WriteJson(List<KeyValuePair<string, string>> headers)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var (name, value) in headers)
            {
                writer.WriteString(name, value);
            }

            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void ReadJson(string json, Dictionary<string, string> headers)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                return;
            }

            foreach (var property in document.RootElement.EnumerateObject())
            {
                if (property.Value.ValueKind == JsonValueKind.String)
                {
                    headers[property.Name] = property.Value.GetString()!;
                }
            }
        }
        catch (JsonException)
        {
            // Headers are best effort; the message itself is still worth delivering.
        }
    }

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}

internal sealed record OutgoingMessage(string Body, IReadOnlyDictionary<string, string> Attributes, string? GroupId, string? DeduplicationId);
