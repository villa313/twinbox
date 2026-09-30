using System.Globalization;
using Azure.Messaging.EventHubs;
using Twinbox.Transport;

namespace Twinbox.EventHubs;

internal static class EventHubsMapping
{
    public const string ErrorProperty = "twinbox-error";

    /// <summary>"eventhub:partition:sequence" of the event a dead-lettered copy came from.</summary>
    public const string OriginProperty = "twinbox-origin";

    private const string FallbackContentType = "application/octet-stream";
    private const int MaxErrorLength = 2000;

    public static EventData ToEventData(TransportMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var result = new EventData(message.Body)
        {
            MessageId = message.MessageId,
            ContentType = message.ContentType,
        };

        foreach (var (name, value) in message.Headers)
        {
            result.Properties[name] = value;
        }

        result.Properties[TransportHeaders.MessageId] = message.MessageId;
        result.Properties[TransportHeaders.MessageName] = message.MessageName;
        return result;
    }

    public static IncomingMessage ToIncomingMessage(EventData data, string eventHub, string partitionId, int deliveryAttempt = 1)
    {
        ArgumentNullException.ThrowIfNull(data);
        var headers = new Dictionary<string, string>(data.Properties.Count, StringComparer.Ordinal);
        foreach (var (name, value) in data.Properties)
        {
            if (ToHeaderValue(value) is { } text)
            {
                headers[name] = text;
            }
        }

        return new IncomingMessage(
            NullIfEmpty(headers.GetValueOrDefault(TransportHeaders.MessageId)) ?? NullIfEmpty(data.MessageId) ?? Coordinates(data, eventHub, partitionId),
            headers.GetValueOrDefault(TransportHeaders.MessageName) ?? string.Empty,
            eventHub,
            data.EventBody.ToMemory(),
            NullIfEmpty(data.ContentType) ?? FallbackContentType,
            headers,
            deliveryAttempt,
            NullIfEmpty(data.PartitionKey) ?? headers.GetValueOrDefault(TransportHeaders.PartitionKey));
    }

    /// <summary>Copies the event with the failure attached; the id is pinned so the copy keeps its identity.</summary>
    public static EventData ToDeadLetter(EventData data, string eventHub, string partitionId, Exception error)
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(error);
        var incoming = ToIncomingMessage(data, eventHub, partitionId);
        var copy = new EventData(data.EventBody)
        {
            MessageId = incoming.MessageId,
            ContentType = data.ContentType,
        };

        foreach (var (name, value) in data.Properties)
        {
            copy.Properties[name] = value;
        }

        var description = $"{error.GetType().Name}: {error.Message}";
        copy.Properties[TransportHeaders.MessageId] = incoming.MessageId;
        copy.Properties[ErrorProperty] = description.Length <= MaxErrorLength ? description : description[..MaxErrorLength];
        copy.Properties[OriginProperty] = Coordinates(data, eventHub, partitionId);
        return copy;
    }

    /// <summary>Stable across redeliveries of the same event, so the inbox can still deduplicate it.</summary>
    public static string Coordinates(EventData data, string eventHub, string partitionId) =>
        string.Create(CultureInfo.InvariantCulture, $"{eventHub}:{partitionId}:{data.SequenceNumber}");

    private static string? ToHeaderValue(object? value) => value switch
    {
        null => null,
        string text => text,
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
