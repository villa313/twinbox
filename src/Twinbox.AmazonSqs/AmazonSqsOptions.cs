using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;

namespace Twinbox.AmazonSqs;

public sealed class AmazonSqsOptions
{
    private readonly List<AmazonSqsListener> _listeners = [];

    /// <summary>System name such as "eu-west-1"; null lets the SDK resolve it from the environment or profile.</summary>
    public string? Region { get; set; }

    /// <summary>Overrides the service endpoint, e.g. http://localhost:4566 for LocalStack.</summary>
    public Uri? ServiceUrl { get; set; }

    /// <summary>Null uses the SDK's default credential chain.</summary>
    public AWSCredentials? Credentials { get; set; }

    /// <summary>Last say over the SQS client configuration.</summary>
    public Action<AmazonSQSConfig>? ConfigureSqs { get; set; }

    /// <summary>Last say over the SNS client configuration.</summary>
    public Action<AmazonSimpleNotificationServiceConfig>? ConfigureSns { get; set; }

    /// <summary>Create missing queues, topics and subscriptions before first use; names ending in ".fifo" become FIFO.</summary>
    public bool AutoCreate { get; set; }

    /// <summary>How long a received message stays hidden from other consumers while it is handled.</summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Long-poll duration per receive, 0 to 20 seconds.</summary>
    public int WaitTimeSeconds { get; set; } = 20;

    /// <summary>Messages fetched per receive, 1 to 10.</summary>
    public int MaxNumberOfMessages { get; set; } = 10;

    /// <summary>Messages handled at once per queue. FIFO queues still handle each message group in order.</summary>
    public int MaxConcurrency { get; set; } = 10;

    /// <summary>Where permanently failing messages are copied before deletion; null leaves them to the queue's redrive policy.</summary>
    public string? DeadLetterQueue { get; set; }

    /// <summary>First delay before a message whose handler failed becomes visible again; doubles up to <see cref="MaxRetryDelay"/>.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>At most 12 hours, the longest visibility timeout SQS accepts.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    internal IReadOnlyList<AmazonSqsListener> Listeners => _listeners;

    /// <summary>Consumes <paramref name="queue"/>, a queue name or URL.</summary>
    public AmazonSqsOptions ListenToQueue(string queue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        _listeners.Add(new AmazonSqsListener(queue, null));
        return this;
    }

    /// <summary>Consumes <paramref name="queue"/> fed by SNS <paramref name="topic"/>; subscribes it with raw delivery when <see cref="AutoCreate"/> is on.</summary>
    public AmazonSqsOptions Subscribe(string queue, string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        _listeners.Add(new AmazonSqsListener(queue, AmazonSqsTransport.StripTopicPrefix(topic)));
        return this;
    }
}

internal sealed record AmazonSqsListener(string Queue, string? Topic);
