using System.Globalization;
using System.Text;
using RabbitMQ.Client;
using Twinbox.Transport;

namespace Twinbox.RabbitMQ;

internal static class RabbitMQMessageMapper
{
    public const string DeliveryCountHeader = "x-delivery-count";

    private const string FallbackContentType = "application/octet-stream";

    public static BasicProperties ToProperties(TransportMessage message)
    {
        var headers = new Dictionary<string, object?>(message.Headers.Count + 1, StringComparer.Ordinal);
        foreach (var (name, value) in message.Headers)
        {
            headers[name] = value;
        }

        if (message.PartitionKey is { } partitionKey)
        {
            headers[TransportHeaders.PartitionKey] = partitionKey;
        }

        return new BasicProperties
        {
            MessageId = message.MessageId,
            Type = message.MessageName,
            ContentType = message.ContentType,
            Persistent = true,
            Headers = headers,
        };
    }

    public static IncomingMessage ToIncoming(string queue, IReadOnlyBasicProperties properties, ReadOnlyMemory<byte> body, bool redelivered)
    {
        var headers = DecodeHeaders(properties.Headers);
        // Left empty when missing: a header profile may still supply the id, and the pipeline rejects it otherwise.
        var messageId = NullIfEmpty(properties.MessageId) ?? headers.GetValueOrDefault(TransportHeaders.MessageId) ?? string.Empty;
        var messageName = NullIfEmpty(properties.Type) ?? headers.GetValueOrDefault(TransportHeaders.MessageName) ?? string.Empty;

        return new IncomingMessage(
            messageId,
            messageName,
            queue,
            body.ToArray(),
            NullIfEmpty(properties.ContentType) ?? FallbackContentType,
            headers,
            DeliveryAttempt(properties.Headers, redelivered),
            headers.GetValueOrDefault(TransportHeaders.PartitionKey));
    }

    private static int DeliveryAttempt(IDictionary<string, object?>? headers, bool redelivered)
    {
        object? raw = null;
        headers?.TryGetValue(DeliveryCountHeader, out raw);

        // Quorum queues count failed deliveries; the redelivered flag is the best guess for other queue types.
        return raw switch
        {
            long count and >= 0 and < int.MaxValue => (int)count + 1,
            int count and >= 0 and < int.MaxValue => count + 1,
            _ => redelivered ? 2 : 1,
        };
    }

    private static Dictionary<string, string> DecodeHeaders(IDictionary<string, object?>? headers)
    {
        var decoded = new Dictionary<string, string>(StringComparer.Ordinal);
        if (headers is null)
        {
            return decoded;
        }

        foreach (var (name, value) in headers)
        {
            if (Decode(value) is { } text)
            {
                decoded[name] = text;
            }
        }

        return decoded;
    }

    // AMQP delivers string headers as UTF-8 byte arrays; tables and arrays (e.g. x-death) have no string form.
    private static string? Decode(object? value) => value switch
    {
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        string text => text,
        bool flag => flag ? "true" : "false",
        AmqpTimestamp timestamp => timestamp.UnixTime.ToString(CultureInfo.InvariantCulture),
        IConvertible convertible => convertible.ToString(CultureInfo.InvariantCulture),
        _ => null,
    };

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
