using Confluent.Kafka;

namespace Twinbox.Kafka;

public sealed class KafkaOptions
{
    private readonly List<KafkaListener> _listeners = [];

    /// <summary>Comma-separated host:port list, e.g. "localhost:9092".</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>Reported to the brokers so operators can tell which app owns a connection.</summary>
    public string ClientId { get; set; } = "twinbox";

    /// <summary>Applied to the producer, consumers and admin client alike, e.g. for SASL or TLS settings.</summary>
    public Action<ClientConfig>? ConfigureClient { get; set; }

    /// <summary>Last say over the producer configuration.</summary>
    public Action<ProducerConfig>? ConfigureProducer { get; set; }

    /// <summary>Last say over each listener's consumer configuration.</summary>
    public Action<ConsumerConfig>? ConfigureConsumer { get; set; }

    /// <summary>Create missing topics (sent to, listened on, and the dead-letter topic) before first use.</summary>
    public bool AutoCreateTopics { get; set; }

    public int TopicPartitions { get; set; } = 1;

    public short TopicReplicationFactor { get; set; } = 1;

    /// <summary>How long a send may wait for the brokers to acknowledge it, retries included.</summary>
    public TimeSpan SendTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Where records that fail permanently are copied before being skipped; null only logs and skips them.</summary>
    public string? DeadLetterTopic { get; set; }

    /// <summary>Where a consumer group without a committed offset starts reading.</summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;

    /// <summary>First delay before a record whose handler failed is retried; doubles up to <see cref="MaxRetryDelay"/>.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Longest wait before a failed record is retried; only its partition pauses meanwhile, the others keep flowing.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Most records of one partition handed to batch handlers at once; a failed batch is retried record by record.</summary>
    public int MaxBatchSize { get; set; } = 1;

    /// <summary>How long to wait for more records to fill a batch; only used when <see cref="MaxBatchSize"/> is above 1.</summary>
    public TimeSpan MaxBatchWait { get; set; } = TimeSpan.FromMilliseconds(50);

    internal IReadOnlyList<KafkaListener> Listeners => _listeners;

    /// <summary>Consumes <paramref name="topic"/> as a member of consumer group <paramref name="groupId"/>.</summary>
    public KafkaOptions Listen(string topic, string groupId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        _listeners.Add(new KafkaListener(topic, groupId));
        return this;
    }
}

internal sealed record KafkaListener(string Topic, string GroupId);
