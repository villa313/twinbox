using NATS.Client.Core;

namespace Twinbox.Nats;

public sealed class NatsOptions
{
    private readonly List<NatsListener> _listeners = [];
    private readonly List<NatsStream> _streams = [];

    /// <summary>Server URL, or a comma-separated list of them, e.g. "nats://localhost:4222".</summary>
    public string Url { get; set; } = "nats://localhost:4222";

    /// <summary>Reported to the server so operators can tell which app owns a connection.</summary>
    public string ClientName { get; set; } = "twinbox";

    /// <summary>Last say over the connection options, e.g. for credentials, NKeys, JWTs or TLS.</summary>
    public Func<NatsOpts, NatsOpts>? ConfigureConnection { get; set; }

    /// <summary>Create the streams registered with <see cref="AddStream"/> before first use; leave off when streams are managed elsewhere.</summary>
    public bool AutoCreateStreams { get; set; }

    /// <summary>How long streams created here remember a Nats-Msg-Id, so a repeated send within it is dropped by the server.</summary>
    public TimeSpan DuplicateWindow { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>Sends to a subject no stream captures are retried by default, since the stream may not exist yet when the
    /// publisher starts. Set this to dead-letter them on the first attempt instead.</summary>
    public bool DeadLetterUnroutable { get; set; }

    /// <summary>Where messages that fail permanently are copied before being terminated; null only logs and terminates them.</summary>
    public string? DeadLetterSubject { get; set; }

    /// <summary>How long the server waits for an acknowledgement before redelivering a message.</summary>
    public TimeSpan AckWait { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Deliveries per message, the first included; the last failed one dead-letters it.</summary>
    public int MaxDeliveryAttempts { get; set; } = 10;

    /// <summary>Unacknowledged messages the server lets a consumer have outstanding across all instances.</summary>
    public int MaxAckPending { get; set; } = 1000;

    /// <summary>Messages each listener pulls ahead of processing; AckWait runs for them while they wait.</summary>
    public int PrefetchCount { get; set; } = 20;

    /// <summary>Messages processed in parallel per listener; above 1, ordering within a subject is lost.</summary>
    public int ConsumerConcurrency { get; set; } = 1;

    /// <summary>First redelivery delay after a handler fails; doubles up to <see cref="MaxRetryDelay"/>.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    internal IReadOnlyList<NatsListener> Listeners => _listeners;

    internal IReadOnlyList<NatsStream> Streams => _streams;

    /// <summary>Consumes <paramref name="stream"/> through a durable pull consumer, optionally narrowed to
    /// <paramref name="filterSubject"/>, which may use the <c>*</c> and <c>&gt;</c> wildcards.</summary>
    public NatsOptions Listen(string stream, string durableConsumer, string? filterSubject = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentException.ThrowIfNullOrWhiteSpace(durableConsumer);
        if (filterSubject is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(filterSubject);
        }

        _listeners.Add(new NatsListener(stream, durableConsumer, filterSubject));
        return this;
    }

    /// <summary>A stream capturing <paramref name="subjects"/>, created when <see cref="AutoCreateStreams"/> is on.</summary>
    public NatsOptions AddStream(string name, params string[] subjects)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(subjects);
        if (subjects.Length == 0 || Array.Exists(subjects, string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("A stream needs at least one non-blank subject.", nameof(subjects));
        }

        _streams.Add(new NatsStream(name, [.. subjects]));
        return this;
    }
}

internal sealed record NatsListener(string Stream, string DurableConsumer, string? FilterSubject);

internal sealed record NatsStream(string Name, IReadOnlyList<string> Subjects);
