using RabbitMQ.Client;

namespace Twinbox.RabbitMQ;

public sealed class RabbitMQOptions
{
    private readonly List<RabbitMQListener> _listeners = [];

    /// <summary>An amqp:// or amqps:// URI; when set it overrides the host, port, virtual host and credentials.</summary>
    public Uri? ConnectionUri { get; set; }

    public string HostName { get; set; } = "localhost";

    /// <summary>Null uses the protocol default (5672, or 5671 for TLS).</summary>
    public int? Port { get; set; }

    public string VirtualHost { get; set; } = "/";

    public string UserName { get; set; } = "guest";

    public string Password { get; set; } = "guest";

    /// <summary>Shown in the management UI so operators can tell which app owns the connection.</summary>
    public string ClientProvidedName { get; set; } = "twinbox";

    /// <summary>Last say over the connection factory, e.g. for TLS or credential providers.</summary>
    public Action<ConnectionFactory>? ConfigureConnectionFactory { get; set; }

    /// <summary>Declare exchanges, queues and bindings on first use; turn off when topology is managed elsewhere.</summary>
    public bool AutoProvision { get; set; } = true;

    /// <summary>
    /// Unroutable publishes are retried by default, because a consumer may not have bound its queue yet when the
    /// publisher starts. Set this to dead-letter them on the first attempt instead.
    /// </summary>
    public bool DeadLetterUnroutable { get; set; }

    /// <summary>Deliveries per message, the first included; the last failed one dead-letters it. Auto-provisioned quorum
    /// queues also get this as their x-delivery-limit, as a backstop for deliveries that never report back.</summary>
    public int MaxDeliveryAttempts { get; set; } = 10;

    /// <summary>First wait before a message whose handler failed goes back to the queue; doubles up to <see cref="MaxRetryDelay"/>.
    /// The message is held unacknowledged meanwhile, so it counts against <see cref="PrefetchCount"/>.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Unacknowledged deliveries the broker sends each listener ahead of processing, up to 65535.</summary>
    public int PrefetchCount { get; set; } = 20;

    /// <summary>Deliveries processed in parallel per listener, up to 65535; above 1, ordering within a queue is lost.</summary>
    public int ConsumerConcurrency { get; set; } = 1;

    /// <summary>Channels kept for publishing; a channel carries one publish at a time.</summary>
    public int PublishChannelPoolSize { get; set; } = Environment.ProcessorCount;

    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(30);

    internal IReadOnlyList<RabbitMQListener> Listeners => _listeners;

    /// <summary>Consumes <paramref name="queue"/>, bound to <paramref name="exchange"/> with <paramref name="bindingKey"/>.</summary>
    public RabbitMQOptions Listen(string queue, string exchange, string bindingKey = "#")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentException.ThrowIfNullOrWhiteSpace(exchange);
        ArgumentNullException.ThrowIfNull(bindingKey);
        _listeners.Add(new RabbitMQListener(queue, exchange, bindingKey));
        return this;
    }
}

internal sealed record RabbitMQListener(string Queue, string Exchange, string BindingKey);
