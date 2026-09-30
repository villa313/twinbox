using System.Globalization;
using NATS.Client.Core;
using Twinbox.Transport;

namespace Twinbox.Nats;

internal static class NatsMapping
{
    /// <summary>JetStream drops a second message with the same value inside the stream's duplicate window.</summary>
    public const string MsgIdHeader = "Nats-Msg-Id";

    public const string ContentTypeHeader = "content-type";
    public const string ErrorHeader = "twinbox-error";

    /// <summary>"stream:sequence" of the message a dead-lettered copy came from.</summary>
    public const string OriginHeader = "twinbox-origin";

    private const string FallbackContentType = "application/octet-stream";
    private const int MaxErrorLength = 2000;

    public static NatsHeaders ToHeaders(TransportMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var headers = new NatsHeaders();
        foreach (var (name, value) in message.Headers)
        {
            headers[name] = value;
        }

        headers[MsgIdHeader] = message.MessageId;
        headers[TransportHeaders.MessageId] = message.MessageId;
        headers[TransportHeaders.MessageName] = message.MessageName;
        headers[ContentTypeHeader] = message.ContentType;
        if (message.PartitionKey is { } partitionKey)
        {
            headers[TransportHeaders.PartitionKey] = partitionKey;
        }

        return headers;
    }

    /// <summary>Falls back to the <paramref name="origin"/> stream coordinates for the id, as they survive redelivery.</summary>
    public static IncomingMessage ToIncomingMessage(
        string subject,
        ReadOnlyMemory<byte> body,
        NatsHeaders? natsHeaders,
        int deliveryAttempt,
        string origin)
    {
        ArgumentNullException.ThrowIfNull(subject);
        var headers = DecodeHeaders(natsHeaders);

        return new IncomingMessage(
            NullIfEmpty(headers.GetValueOrDefault(TransportHeaders.MessageId))
                ?? NullIfEmpty(headers.GetValueOrDefault(MsgIdHeader))
                ?? origin,
            headers.GetValueOrDefault(TransportHeaders.MessageName) ?? string.Empty,
            subject,
            body,
            NullIfEmpty(headers.GetValueOrDefault(ContentTypeHeader)) ?? FallbackContentType,
            headers,
            Math.Max(deliveryAttempt, 1),
            NullIfEmpty(headers.GetValueOrDefault(TransportHeaders.PartitionKey)));
    }

    /// <summary>Copies the headers with the failure attached; the copy gets its own Nats-Msg-Id but keeps the Twinbox id.</summary>
    public static NatsHeaders ToDeadLetterHeaders(IncomingMessage message, string origin, Exception error)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(error);
        var headers = new NatsHeaders();
        foreach (var (name, value) in message.Headers)
        {
            if (name is not (ErrorHeader or OriginHeader or MsgIdHeader))
            {
                headers[name] = value;
            }
        }

        var description = $"{error.GetType().Name}: {error.Message}";
        headers[TransportHeaders.MessageId] = message.MessageId;
        headers[MsgIdHeader] = $"dead-letter:{origin}";
        headers[ErrorHeader] = SingleLine(description.Length <= MaxErrorLength ? description : description[..MaxErrorLength]);
        headers[OriginHeader] = origin;
        return headers;
    }

    public static string Origin(string stream, ulong sequence) =>
        string.Create(CultureInfo.InvariantCulture, $"{stream}:{sequence}");

    private static Dictionary<string, string> DecodeHeaders(NatsHeaders? headers)
    {
        var decoded = new Dictionary<string, string>(StringComparer.Ordinal);
        if (headers is null)
        {
            return decoded;
        }

        foreach (var (name, values) in headers)
        {
            if (values.Count > 0 && values[^1] is { } value)
            {
                decoded[name] = value;
            }
        }

        return decoded;
    }

    // Header values travel on a single protocol line, so line breaks from exception messages must go.
    private static string SingleLine(string text) => text.ReplaceLineEndings(" ");

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
