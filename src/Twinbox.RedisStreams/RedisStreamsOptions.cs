using StackExchange.Redis;

namespace Twinbox.RedisStreams;

public sealed class RedisStreamsOptions
{
    private readonly List<RedisStreamsListener> _listeners = [];

    /// <summary>A StackExchange.Redis configuration string, e.g. "localhost:6379,password=secret".</summary>
    public string? Configuration { get; set; }

    /// <summary>Supplies a multiplexer the app already owns, taking precedence over <see cref="Configuration"/>; Twinbox never disposes it.</summary>
    public Func<IServiceProvider, IConnectionMultiplexer>? ConnectionFactory { get; set; }

    /// <summary>Caps each stream sent to at roughly this many entries (XADD MAXLEN ~); null keeps every entry.</summary>
    public int? MaxLength { get; set; }

    /// <summary>Idle time before any group member reclaims an unacknowledged entry; also the retry delay, so keep it above handler run time.</summary>
    public TimeSpan ClaimIdleAfter { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Deliveries of an entry before it is moved to the dead stream.</summary>
    public int MaxDeliveries { get; set; } = 10;

    /// <summary>Entries read or reclaimed per round trip.</summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>Wait before reading again after a read found nothing new.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>This process's name within each consumer group; must differ between instances sharing a group.</summary>
    public string ConsumerName { get; set; } = $"{Environment.MachineName}-{Environment.ProcessId}";

    internal IReadOnlyList<RedisStreamsListener> Listeners => _listeners;

    /// <summary>Consumes <paramref name="stream"/> as a member of <paramref name="group"/>, creating both when missing.</summary>
    public RedisStreamsOptions Listen(string stream, string group)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stream);
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        _listeners.Add(new RedisStreamsListener(stream, group));
        return this;
    }
}

internal sealed record RedisStreamsListener(string Stream, string Group)
{
    public string DeadStream => RedisStreamsMapping.DeadStream(Stream);
}
