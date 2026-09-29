using System.Buffers;
using System.Globalization;
using DotPulsar;
using DotPulsar.Abstractions;
using Twinbox.Transport;

namespace Twinbox.Pulsar;

internal static class PulsarMapping
{
    public const string ContentTypeProperty = "content-type";
    public const string ErrorProperty = "twinbox-error";

    /// <summary>"topic:ledger:entry:partition:batch" of the message a dead-lettered copy came from.</summary>
    public const string OriginProperty = "twinbox-origin";

    /// <summary>The subscription that gave up on a dead-lettered copy; several subscriptions share one dead-letter topic.</summary>
    public const string SubscriptionProperty = "twinbox-subscription";

    private const string FallbackContentType = "application/octet-stream";
    private const int MaxErrorLength = 2000;

    public static string DeadLetterTopic(string topic, string suffix) => topic + suffix;

    public static MessageMetadata ToMetadata(TransportMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        // The sequence id is left to the producer: Twinbox ids aren't monotonic, and broker deduplication drops any id
        // at or below the highest it has seen, so reusing them would silently lose retried messages.
        var metadata = new MessageMetadata();
        foreach (var (name, value) in message.Headers)
        {
            if (name is not (TransportHeaders.MessageId or TransportHeaders.MessageName or ContentTypeProperty))
            {
                metadata[name] = value;
            }
        }

        metadata[TransportHeaders.MessageId] = message.MessageId;
        metadata[TransportHeaders.MessageName] = message.MessageName;
        metadata[ContentTypeProperty] = message.ContentType;
        if (message.PartitionKey is not null)
        {
            metadata.Key = message.PartitionKey;
        }

        return metadata;
    }

    public static IncomingMessage ToIncomingMessage(string topic, IMessage message, int deliveryAttempt = 1)
    {
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(message);
        var headers = new Dictionary<string, string>(message.Properties, StringComparer.Ordinal);

        return new IncomingMessage(
            IdOf(topic, message),
            headers.GetValueOrDefault(TransportHeaders.MessageName) ?? string.Empty,
            topic,
            message.Data.ToArray(),
            NullIfEmpty(headers.GetValueOrDefault(ContentTypeProperty)) ?? FallbackContentType,
            headers,
            deliveryAttempt,
            message.HasKey ? message.Key : null);
    }

    /// <summary>Copies the message with the failure attached; the id property is pinned so the copy keeps its identity.</summary>
    public static MessageMetadata ToDeadLetter(string topic, string subscription, IMessage message, string error)
    {
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(subscription);
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(error);
        var metadata = new MessageMetadata();
        foreach (var (name, value) in message.Properties)
        {
            if (name is not (ErrorProperty or OriginProperty or SubscriptionProperty))
            {
                metadata[name] = value;
            }
        }

        metadata[TransportHeaders.MessageId] = IdOf(topic, message);
        metadata[ErrorProperty] = error.Length <= MaxErrorLength ? error : error[..MaxErrorLength];
        metadata[OriginProperty] = Coordinates(topic, message.MessageId);
        metadata[SubscriptionProperty] = subscription;
        if (message.HasBase64EncodedKey)
        {
            metadata.KeyBytes = message.KeyBytes;
        }
        else if (message.HasKey)
        {
            metadata.Key = message.Key;
        }

        return metadata;
    }

    /// <summary>The broker only counts redeliveries on Shared and KeyShared subscriptions, so a local tally can raise it.</summary>
    public static int DeliveryAttempt(IMessage message, int? failedAttempt = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        var attempt = (int)Math.Min(message.RedeliveryCount, int.MaxValue - 1u) + 1;
        return failedAttempt is { } failed && failed >= attempt ? failed + 1 : attempt;
    }

    public static string Describe(Exception error) => $"{error.GetType().Name}: {error.Message}";

    /// <summary>Stable across redeliveries of the same message, so the inbox can still deduplicate it.</summary>
    public static string Coordinates(string topic, MessageId id) =>
        string.Create(CultureInfo.InvariantCulture, $"{topic}:{id.LedgerId}:{id.EntryId}:{id.Partition}:{id.BatchIndex}");

    public static string IdOf(string topic, IMessage message) =>
        NullIfEmpty(message.Properties.GetValueOrDefault(TransportHeaders.MessageId)) ?? Coordinates(topic, message.MessageId);

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
