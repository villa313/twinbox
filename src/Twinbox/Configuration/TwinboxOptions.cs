namespace Twinbox;

/// <summary>Bound from the "Twinbox" configuration section; configuration values override code.</summary>
public sealed class TwinboxOptions
{
    public const string SectionName = "Twinbox";

    /// <summary>Identifies this process as a lease owner; must be unique per running instance.</summary>
    public string InstanceId { get; set; } = $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";

    public DispatcherOptions Dispatcher { get; set; } = new();

    public RetryOptions Retry { get; set; } = new();

    public CircuitBreakerOptions CircuitBreaker { get; set; } = new();

    /// <summary>Per-destination overrides, keyed by destination name.</summary>
    public Dictionary<string, DestinationOptions> Destinations { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public RetentionOptions Retention { get; set; } = new();

    public InboxOptions Inbox { get; set; } = new();
}

public sealed class DispatcherOptions
{
    /// <summary>Turn off to host dispatching elsewhere, e.g. an Azure Functions timer calling <see cref="IOutboxDispatcher"/>.</summary>
    public bool Enabled { get; set; } = true;

    public int BatchSize { get; set; } = 100;

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan MinPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxDegreeOfParallelism { get; set; } = Environment.ProcessorCount;
}

public sealed class RetryOptions
{
    public int MaxAttempts { get; set; } = 10;

    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);
}

public sealed class CircuitBreakerOptions
{
    /// <summary>Consecutive failures that open the circuit; 0 disables the breaker.</summary>
    public int FailureThreshold { get; set; } = 5;

    public TimeSpan BreakDuration { get; set; } = TimeSpan.FromSeconds(30);
}

public sealed class DestinationOptions
{
    public RetryOptions? Retry { get; set; }

    public CircuitBreakerOptions? CircuitBreaker { get; set; }
}

public sealed class RetentionOptions
{
    /// <summary>Turn off in hosts that only send, so a single host owns cleanup.</summary>
    public bool Enabled { get; set; } = true;

    public TimeSpan SentMessages { get; set; } = TimeSpan.FromDays(1);

    /// <summary>Null keeps dead messages until someone replays or deletes them.</summary>
    public TimeSpan? DeadMessages { get; set; }

    /// <summary>How long duplicates are still detected after a message was processed.</summary>
    public TimeSpan InboxEntries { get; set; } = TimeSpan.FromDays(7);

    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(5);

    public int BatchSize { get; set; } = 1000;
}

public sealed class InboxOptions
{
    public bool Enabled { get; set; } = true;

    /// <summary>What to do with a message this app has no type or no handler for.</summary>
    public UnknownMessagePolicy UnknownMessages { get; set; } = UnknownMessagePolicy.DeadLetter;
}

public enum UnknownMessagePolicy
{
    DeadLetter = 0,
    Ignore = 1,
}
