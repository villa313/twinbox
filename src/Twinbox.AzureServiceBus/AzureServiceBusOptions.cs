using System.Diagnostics.CodeAnalysis;

namespace Twinbox.AzureServiceBus;

public sealed class AzureServiceBusOptions
{
    private readonly List<AzureServiceBusListener> _listeners = [];

    /// <summary>Sets SessionId on sent messages, as session-enabled entities require: the partition key, or the message id
    /// when there is none. Entities without sessions ignore it, though partitioned ones then pin each id to a partition.</summary>
    public bool SendSessionIds { get; set; }

    /// <summary>Messages handled concurrently per listener; for session listeners, the number of sessions handled concurrently.</summary>
    public int MaxConcurrentCalls { get; set; } = 1;

    public int PrefetchCount { get; set; }

    /// <summary>First wait before a message whose handler failed is abandoned for redelivery; doubles up to <see cref="MaxRetryDelay"/>.
    /// The message stays locked meanwhile and holds one of <see cref="MaxConcurrentCalls"/>.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>At most 5 minutes, the processor's lock renewal limit.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    internal IReadOnlyList<AzureServiceBusListener> Listeners => _listeners;

    /// <summary>Consumes <paramref name="queueName"/>; with <paramref name="sessions"/>, through a session processor, which session-enabled queues require.</summary>
    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required parameter, so calls cannot be ambiguous.")]
    [SuppressMessage("ApiDesign", "RS0027", Justification = "Each overload names a different kind of entity.")]
    public AzureServiceBusOptions Listen(string queueName, bool sessions = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        _listeners.Add(new AzureServiceBusListener(queueName, null, sessions));
        return this;
    }

    /// <summary>Consumes a topic subscription; with <paramref name="sessions"/>, through a session processor, which session-enabled subscriptions require.</summary>
    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required parameter, so calls cannot be ambiguous.")]
    [SuppressMessage("ApiDesign", "RS0027", Justification = "Each overload names a different kind of entity.")]
    public AzureServiceBusOptions Listen(string topicName, string subscriptionName, bool sessions = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionName);
        _listeners.Add(new AzureServiceBusListener(topicName, subscriptionName, sessions));
        return this;
    }
}

/// <summary>A queue, or a topic subscription when <see cref="SubscriptionName"/> is set.</summary>
internal sealed record AzureServiceBusListener(string EntityName, string? SubscriptionName, bool Sessions)
{
    /// <summary>Reported to handlers as the message source.</summary>
    public string EntityPath => SubscriptionName is null ? EntityName : $"{EntityName}/Subscriptions/{SubscriptionName}";
}
