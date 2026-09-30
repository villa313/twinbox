using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Twinbox.Transport;

namespace Twinbox.GooglePubSub;

internal static class GooglePubSubMapping
{
    public const string ContentTypeAttribute = "twinbox-content-type";
    public const string ErrorAttribute = "twinbox-error";

    /// <summary>Subscription a dead-lettered copy came from.</summary>
    public const string OriginAttribute = "twinbox-origin";

    private const string FallbackContentType = "application/octet-stream";

    // Pub/Sub caps attribute values at 1024 bytes; 340 chars stay under it even at 3 bytes each.
    private const int MaxErrorLength = 340;

    public static bool IsFullName(string name) => name.StartsWith("projects/", StringComparison.Ordinal);

    public static TopicName ToTopicName(string topic, string? projectId) =>
        IsFullName(topic) ? TopicName.Parse(topic) : new TopicName(RequireProject(projectId, topic), topic);

    public static SubscriptionName ToSubscriptionName(string subscription, string? projectId) =>
        IsFullName(subscription) ? SubscriptionName.Parse(subscription) : new SubscriptionName(RequireProject(projectId, subscription), subscription);

    public static PubsubMessage ToPubsubMessage(TransportMessage message, bool ordering)
    {
        ArgumentNullException.ThrowIfNull(message);
        var outgoing = new PubsubMessage { Data = ByteString.CopyFrom(message.Body.Span) };
        foreach (var (name, value) in message.Headers)
        {
            outgoing.Attributes[name] = value;
        }

        outgoing.Attributes[TransportHeaders.MessageId] = message.MessageId;
        outgoing.Attributes[TransportHeaders.MessageName] = message.MessageName;
        outgoing.Attributes[ContentTypeAttribute] = message.ContentType;
        if (!string.IsNullOrEmpty(message.PartitionKey))
        {
            outgoing.Attributes[TransportHeaders.PartitionKey] = message.PartitionKey;
            if (ordering)
            {
                outgoing.OrderingKey = message.PartitionKey;
            }
        }

        return outgoing;
    }

    public static IncomingMessage ToIncoming(PubsubMessage message, string subscription)
    {
        ArgumentNullException.ThrowIfNull(message);
        var headers = new Dictionary<string, string>(message.Attributes, StringComparer.Ordinal);

        return new IncomingMessage(
            NullIfEmpty(headers.GetValueOrDefault(TransportHeaders.MessageId)) ?? message.MessageId,
            headers.GetValueOrDefault(TransportHeaders.MessageName) ?? string.Empty,
            subscription,
            message.Data.Memory,
            NullIfEmpty(headers.GetValueOrDefault(ContentTypeAttribute)) ?? FallbackContentType,
            headers,
            message.GetDeliveryAttempt() ?? 1,
            NullIfEmpty(message.OrderingKey) ?? NullIfEmpty(headers.GetValueOrDefault(TransportHeaders.PartitionKey)));
    }

    /// <summary>Copies the message with the failure attached; the id attribute is pinned so the copy keeps its identity.</summary>
    public static PubsubMessage ToDeadLetter(PubsubMessage message, IncomingMessage incoming, Exception error)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(incoming);
        ArgumentNullException.ThrowIfNull(error);
        var copy = new PubsubMessage { Data = message.Data };
        copy.Attributes.Add(message.Attributes);
        var description = $"{error.GetType().Name}: {error.Message}";
        copy.Attributes[TransportHeaders.MessageId] = incoming.MessageId;
        copy.Attributes[ErrorAttribute] = description.Length <= MaxErrorLength ? description : description[..MaxErrorLength];
        copy.Attributes[OriginAttribute] = incoming.Source;
        if (incoming.PartitionKey is { } partitionKey)
        {
            copy.Attributes[TransportHeaders.PartitionKey] = partitionKey;
        }

        return copy;
    }

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;

    private static string RequireProject(string? projectId, string name) =>
        string.IsNullOrWhiteSpace(projectId)
            ? throw new ArgumentException($"'{name}' is a short name, so GooglePubSubOptions.ProjectId must be set.", nameof(projectId))
            : projectId;
}
