using System.Text.Json;
using StackExchange.Redis;
using Twinbox.Serialization;
using Twinbox.Transport;

namespace Twinbox.RedisStreams;

internal static class RedisStreamsMapping
{
    public const string IdField = "id";
    public const string NameField = "name";
    public const string ContentTypeField = "content-type";
    public const string BodyField = "body";
    public const string HeadersField = "headers";
    public const string ErrorField = "error";

    /// <summary>"stream:entryId" of the entry a dead copy came from.</summary>
    public const string OriginField = "origin";

    /// <summary>The consumer group that gave up on a dead copy; several groups share one dead stream.</summary>
    public const string GroupField = "group";

    private const string FallbackContentType = "application/octet-stream";
    private const int MaxErrorLength = 2000;

    public static string DeadStream(string stream) => $"{stream}:dead";

    public static NameValueEntry[] ToEntry(TransportMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in message.Headers)
        {
            if (name is not (TransportHeaders.MessageId or TransportHeaders.MessageName or TransportHeaders.PartitionKey))
            {
                headers[name] = value;
            }
        }

        // Streams have no key of their own, so the partition key rides along as a header.
        if (message.PartitionKey is not null)
        {
            headers[TransportHeaders.PartitionKey] = message.PartitionKey;
        }

        return
        [
            new(IdField, message.MessageId),
            new(NameField, message.MessageName),
            new(ContentTypeField, message.ContentType),
            new(BodyField, message.Body),
            new(HeadersField, HeaderCodec.Encode(headers) ?? string.Empty),
        ];
    }

    /// <summary>Throws <see cref="PermanentDeliveryException"/> for headers no retry can parse.</summary>
    public static IncomingMessage ToIncomingMessage(string stream, StreamEntry entry, int deliveryAttempt = 1)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var messageId = MessageId(stream, entry);
        IReadOnlyDictionary<string, string> headers;
        try
        {
            headers = HeaderCodec.Decode(Text(entry, HeadersField));
        }
        catch (JsonException ex)
        {
            throw new PermanentDeliveryException($"Entry {entry.Id} of stream '{stream}' has unreadable headers.", ex);
        }

        return new IncomingMessage(
            messageId,
            Text(entry, NameField) ?? string.Empty,
            stream,
            (byte[]?)entry[BodyField] ?? [],
            Text(entry, ContentTypeField) ?? FallbackContentType,
            headers,
            deliveryAttempt,
            headers.GetValueOrDefault(TransportHeaders.PartitionKey));
    }

    /// <summary>Copies the entry with the failure attached; the id is pinned so the copy keeps its identity.</summary>
    public static NameValueEntry[] ToDeadLetter(string stream, string group, StreamEntry entry, string error)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(error);
        var fields = new List<NameValueEntry>(entry.Values?.Length + 4 ?? 4)
        {
            new(IdField, MessageId(stream, entry)),
        };

        foreach (var field in entry.Values ?? [])
        {
            if ((string?)field.Name is not (IdField or ErrorField or OriginField or GroupField))
            {
                fields.Add(field);
            }
        }

        fields.Add(new(ErrorField, error.Length <= MaxErrorLength ? error : error[..MaxErrorLength]));
        fields.Add(new(OriginField, Coordinates(stream, entry.Id)));
        fields.Add(new(GroupField, group));
        return [.. fields];
    }

    public static string Describe(Exception error) => $"{error.GetType().Name}: {error.Message}";

    /// <summary>Stable across redeliveries of the same entry, so the inbox can still deduplicate it.</summary>
    public static string Coordinates(string stream, RedisValue entryId) => $"{stream}:{entryId}";

    public static string MessageId(string stream, StreamEntry entry) => Text(entry, IdField) ?? Coordinates(stream, entry.Id);

    private static string? Text(StreamEntry entry, string field) => entry[field] is { IsNullOrEmpty: false } value ? (string?)value : null;
}
