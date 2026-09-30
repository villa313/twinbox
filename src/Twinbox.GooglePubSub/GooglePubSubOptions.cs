using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;

namespace Twinbox.GooglePubSub;

public sealed class GooglePubSubOptions
{
    private readonly List<GooglePubSubSubscription> _subscriptions = [];

    /// <summary>Project that short topic and subscription names belong to; full "projects/…/topics/…" names may point
    /// elsewhere. Required once any short name is used.</summary>
    public string? ProjectId { get; set; }

    /// <summary>host:port of the Pub/Sub emulator, reached without credentials; when null, PUBSUB_EMULATOR_HOST is still honoured.</summary>
    public string? EmulatorHost { get; set; }

    /// <summary>Null uses Application Default Credentials.</summary>
    public GoogleCredential? Credential { get; set; }

    /// <summary>Last say over each publisher, e.g. for batching or custom credentials.</summary>
    public Action<PublisherClientBuilder>? ConfigurePublisher { get; set; }

    /// <summary>Last say over each subscriber, e.g. for client count or custom credentials.</summary>
    public Action<SubscriberClientBuilder>? ConfigureSubscriber { get; set; }

    /// <summary>Create missing topics and subscriptions before first use; existing ones are left as they are.</summary>
    public bool AutoCreate { get; set; }

    /// <summary>How long a received message stays leased before Pub/Sub redelivers it; the subscriber extends it while a handler runs.</summary>
    public TimeSpan AckDeadline { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Messages leased but not yet acknowledged per subscription, which bounds how many handlers run at once.</summary>
    public long MaxOutstandingMessages { get; set; } = 100;

    /// <summary>Publishes with the partition key as ordering key so each key arrives in order; auto-created subscriptions match.</summary>
    public bool EnableMessageOrdering { get; set; }

    /// <summary>Receives copies of permanently failing messages. Pub/Sub drops what reaches a topic without subscriptions.</summary>
    public string? DeadLetterTopic { get; set; }

    /// <summary>Deliveries, the first included, before an auto-created subscription forwards a message to
    /// <see cref="DeadLetterTopic"/>; 5 to 100. Only used when both are set.</summary>
    public int MaxDeliveryAttempts { get; set; } = 10;

    internal IReadOnlyList<GooglePubSubSubscription> Subscriptions => _subscriptions;

    /// <summary>Consumes <paramref name="subscription"/> of <paramref name="topic"/>; the topic is only used to create the subscription.</summary>
    public GooglePubSubOptions Listen(string subscription, string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        _subscriptions.Add(new GooglePubSubSubscription(subscription, topic));
        return this;
    }
}

internal sealed record GooglePubSubSubscription(string Subscription, string Topic);
