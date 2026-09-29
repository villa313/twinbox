using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Confluent.Kafka;
using Twinbox.Transport;

namespace Twinbox.Kafka;

internal static class KafkaMapping
{
    public const string ContentTypeHeader = "content-type";
    public const string ErrorHeader = "twinbox-error";

    /// <summary>"topic:partition:offset" of the record a dead-lettered copy came from.</summary>
    public const string OriginHeader = "twinbox-origin";

    private const string FallbackContentType = "application/octet-stream";
    private const int MaxErrorLength = 2000;

    public static Message<string?, byte[]> ToKafkaMessage(TransportMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var headers = new Headers();
        foreach (var (name, value) in message.Headers)
        {
            if (name is not (TransportHeaders.MessageId or TransportHeaders.MessageName or ContentTypeHeader))
            {
                headers.Add(name, Encoding.UTF8.GetBytes(value));
            }
        }

        headers.Add(TransportHeaders.MessageId, Encoding.UTF8.GetBytes(message.MessageId));
        headers.Add(TransportHeaders.MessageName, Encoding.UTF8.GetBytes(message.MessageName));
        headers.Add(ContentTypeHeader, Encoding.UTF8.GetBytes(message.ContentType));

        return new Message<string?, byte[]>
        {
            Key = message.PartitionKey,
            Value = ToArray(message.Body),
            Headers = headers,
        };
    }

    public static IncomingMessage ToIncomingMessage(ConsumeResult<string?, byte[]> record, int deliveryAttempt = 1)
    {
        ArgumentNullException.ThrowIfNull(record);
        var headers = DecodeHeaders(record.Message.Headers);

        return new IncomingMessage(
            NullIfEmpty(headers.GetValueOrDefault(TransportHeaders.MessageId)) ?? Coordinates(record),
            headers.GetValueOrDefault(TransportHeaders.MessageName) ?? string.Empty,
            record.Topic,
            record.Message.Value ?? [],
            NullIfEmpty(headers.GetValueOrDefault(ContentTypeHeader)) ?? FallbackContentType,
            headers,
            deliveryAttempt,
            record.Message.Key);
    }

    /// <summary>Copies the record with the failure attached; the id header is pinned so the copy keeps its identity.</summary>
    public static Message<string?, byte[]> ToDeadLetter(ConsumeResult<string?, byte[]> record, Exception error)
    {
        ArgumentNullException.ThrowIfNull(record);
        ArgumentNullException.ThrowIfNull(error);
        var headers = new Headers();
        var messageId = Coordinates(record);
        foreach (var header in record.Message.Headers ?? [])
        {
            if (header.Key is ErrorHeader or OriginHeader)
            {
                continue;
            }

            if (header.Key == TransportHeaders.MessageId && header.GetValueBytes() is { Length: > 0 } id)
            {
                messageId = Encoding.UTF8.GetString(id);
                continue;
            }

            headers.Add(header.Key, header.GetValueBytes());
        }

        var description = $"{error.GetType().Name}: {error.Message}";
        headers.Add(TransportHeaders.MessageId, Encoding.UTF8.GetBytes(messageId));
        headers.Add(ErrorHeader, Encoding.UTF8.GetBytes(description.Length <= MaxErrorLength ? description : description[..MaxErrorLength]));
        headers.Add(OriginHeader, Encoding.UTF8.GetBytes(Coordinates(record)));

        return new Message<string?, byte[]>
        {
            Key = record.Message.Key,
            Value = record.Message.Value,
            Headers = headers,
        };
    }

    /// <summary>Stable across redeliveries of the same record, so the inbox can still deduplicate it.</summary>
    public static string Coordinates(ConsumeResult<string?, byte[]> record) =>
        string.Create(CultureInfo.InvariantCulture, $"{record.Topic}:{record.Partition.Value}:{record.Offset.Value}");

    private static Dictionary<string, string> DecodeHeaders(Headers? headers)
    {
        var decoded = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var header in headers ?? [])
        {
            if (header.GetValueBytes() is { } value)
            {
                decoded[header.Key] = Encoding.UTF8.GetString(value);
            }
        }

        return decoded;
    }

    private static byte[] ToArray(ReadOnlyMemory<byte> body) =>
        MemoryMarshal.TryGetArray(body, out var segment) && segment.Offset == 0 && segment.Count == segment.Array!.Length
            ? segment.Array
            : body.ToArray();

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
