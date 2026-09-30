using DotPulsar;
using DotPulsar.Abstractions;

namespace Twinbox.Pulsar;

public sealed class PulsarOptions
{
    private readonly List<PulsarListener> _listeners = [];

    /// <summary>The broker's binary protocol address, e.g. "pulsar://localhost:6650" or "pulsar+ssl://broker:6651".</summary>
    public Uri? ServiceUrl { get; set; }

    /// <summary>Last say over the client, e.g. for authentication, TLS certificates or a listener name.</summary>
    public Action<IPulsarClientBuilder>? ConfigureClient { get; set; }

    /// <summary>Shared spreads messages unordered; KeyShared keeps each partition key in order, though a redelivered failure can
    /// land after later messages of its key. Exclusive and Failover redeliver every unacknowledged message on a retry.</summary>
    public SubscriptionType SubscriptionType { get; set; } = SubscriptionType.Shared;

    /// <summary>Where a subscription created by a listener starts reading.</summary>
    public SubscriptionInitialPosition InitialPosition { get; set; } = SubscriptionInitialPosition.Earliest;

    /// <summary>Deliveries of a message, the first included; the last failed one moves it to the dead-letter topic.</summary>
    public int MaxDeliveryAttempts { get; set; } = 10;

    /// <summary>First wait before a message whose handler failed is handed back to the broker; doubles up to
    /// <see cref="MaxRetryDelay"/>. The consumer holds it unacknowledged meanwhile.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Appended to a listener's topic to name its dead-letter topic.</summary>
    public string DeadLetterSuffix { get; set; } = "-dlq";

    /// <summary>Consumers per listener, each handling one message at a time; above 1 needs a Shared or KeyShared subscription.</summary>
    public int ConsumerConcurrency { get; set; } = 1;

    /// <summary>How long a send may wait for the broker to confirm it, reconnects included.</summary>
    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(30);

    internal IReadOnlyList<PulsarListener> Listeners => _listeners;

    /// <summary>Consumes <paramref name="topic"/> through <paramref name="subscription"/>, creating the subscription when missing.</summary>
    public PulsarOptions Listen(string topic, string subscription)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        _listeners.Add(new PulsarListener(topic, subscription));
        return this;
    }
}

internal sealed record PulsarListener(string Topic, string Subscription);
