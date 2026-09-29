namespace Twinbox.AzureServiceBus;

public sealed class AzureServiceBusOptions
{
    private readonly List<AzureServiceBusListener> _listeners = [];

    /// <summary>Sends the partition key as SessionId and receives with session processors; listened entities must be session-enabled.</summary>
    public bool UseSessions { get; set; }

    /// <summary>Messages handled concurrently per listener; with sessions, the number of sessions handled concurrently.</summary>
    public int MaxConcurrentCalls { get; set; } = 1;

    public int PrefetchCount { get; set; }

    public IReadOnlyList<AzureServiceBusListener> Listeners => _listeners;

    public AzureServiceBusOptions Listen(string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        _listeners.Add(new AzureServiceBusListener(queueName, null));
        return this;
    }

    public AzureServiceBusOptions Listen(string topicName, string subscriptionName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionName);
        _listeners.Add(new AzureServiceBusListener(topicName, subscriptionName));
        return this;
    }
}

/// <summary>A queue, or a topic subscription when <see cref="SubscriptionName"/> is set.</summary>
public sealed record AzureServiceBusListener(string EntityName, string? SubscriptionName)
{
    /// <summary>Reported to handlers as the message source.</summary>
    public string EntityPath => SubscriptionName is null ? EntityName : $"{EntityName}/Subscriptions/{SubscriptionName}";
}
