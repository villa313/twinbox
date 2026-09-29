using RabbitMQ.Client;

namespace Twinbox.RabbitMQ;

public sealed class RabbitMqOptions
{
    private readonly List<RabbitMqListener> _listeners = [];

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

    /// <summary>Deliveries per message before the quorum queue dead-letters it.</summary>
    public int DeliveryLimit { get; set; } = 10;

    public ushort PrefetchCount { get; set; } = 20;

    /// <summary>Deliveries processed in parallel per listener; above 1, ordering within a queue is lost.</summary>
    public ushort ConsumerConcurrency { get; set; } = 1;

    /// <summary>Channels kept for publishing; a channel carries one publish at a time.</summary>
    public int PublishChannelPoolSize { get; set; } = Environment.ProcessorCount;

    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(30);

    internal IReadOnlyList<RabbitMqListener> Listeners => _listeners;

    /// <summary>Consumes <paramref name="queue"/>, bound to <paramref name="exchange"/> with <paramref name="bindingKey"/>.</summary>
    public RabbitMqOptions Listen(string queue, string exchange, string bindingKey = "#")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentException.ThrowIfNullOrWhiteSpace(exchange);
        ArgumentNullException.ThrowIfNull(bindingKey);
        _listeners.Add(new RabbitMqListener(queue, exchange, bindingKey));
        return this;
    }
}

internal sealed record RabbitMqListener(string Queue, string Exchange, string BindingKey);
