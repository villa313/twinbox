using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Chaos.Tests;

public enum Fault
{
    None,
    Transient,
    RetryAfter,
    Timeout,
    Hang,
    Permanent,
}

/// <summary>Faults keyed by message id and attempt alone, so every outcome is known up front whoever sends it, whenever.</summary>
public sealed class FaultPlan(int seed)
{
    public static readonly TimeSpan HangFor = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan RetryAfter = TimeSpan.FromMilliseconds(100);

    public Fault For(int messageId, int attempt)
    {
        var roll = new Random(unchecked((seed * 1_000_003) + (messageId * 7_919) + attempt)).NextDouble();
        return roll switch
        {
            < 0.55 => Fault.None,
            < 0.75 => Fault.Transient,
            < 0.83 => Fault.RetryAfter,
            < 0.90 => Fault.Timeout,
            < 0.95 => Fault.Hang,
            _ => Fault.Permanent,
        };
    }

    /// <summary>Where the retry policy must leave a message, and after how many sends.</summary>
    public (OutboxMessageStatus Status, int Attempts) Expected(int messageId, int maxAttempts)
    {
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            switch (For(messageId, attempt))
            {
                case Fault.None:
                    return (OutboxMessageStatus.Sent, attempt);
                case Fault.Permanent:
                    return (OutboxMessageStatus.Dead, attempt);
            }
        }

        return (OutboxMessageStatus.Dead, maxAttempts);
    }

    public async Task InjectAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        switch (For(DeliveryLog.Read(message).Id, DeliveryLog.AttemptOf(message)))
        {
            case Fault.Transient:
                throw new IOException("Connection reset by broker.");
            case Fault.RetryAfter:
                throw new RetryAfterException("Broker is throttling.", RetryAfter);
            case Fault.Timeout:
                await Task.Delay(Timeout.Infinite, cancellationToken);
                break;
            case Fault.Hang:
                // Ignores the token, like a client stuck in a blocking call; the dispatcher must time out on its own.
                await Task.Delay(HangFor, CancellationToken.None);
                throw new IOException("Broker never answered.");
            case Fault.Permanent:
                throw new PermanentDeliveryException("Broker rejected the message.");
        }
    }
}

/// <summary>A fault that can be switched on and off while dispatchers are running.</summary>
public sealed class Outage
{
    private volatile bool _active;

    public bool Active
    {
        get => _active;
        set => _active = value;
    }

    public Task InjectAsync(TransportMessage message, CancellationToken cancellationToken) =>
        _active ? throw new IOException("Broker unreachable.") : Task.CompletedTask;
}
