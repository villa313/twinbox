using Twinbox.Transport;

namespace Twinbox.AmazonSqs.Tests;

[MessageName("order-placed")]
public sealed record OrderPlaced(int OrderId);

public sealed class Journal
{
    private readonly List<(int OrderId, string? PartitionKey)> _entries = [];

    public IReadOnlyList<int> Handled
    {
        get
        {
            lock (_entries)
            {
                return [.. _entries.Select(e => e.OrderId)];
            }
        }
    }

    public IReadOnlyList<(int OrderId, string? PartitionKey)> Entries
    {
        get
        {
            lock (_entries)
            {
                return [.. _entries];
            }
        }
    }

    public void Add(int orderId, string? partitionKey)
    {
        lock (_entries)
        {
            _entries.Add((orderId, partitionKey));
        }
    }

    public async Task WaitForAsync(int orderId, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!Handled.Contains(orderId))
        {
            await Task.Delay(50, cts.Token);
        }
    }
}

/// <summary>Makes handlers fail for held orders until the test releases them.</summary>
public sealed class FailureGate
{
    private readonly HashSet<int> _held = [];
    private int _failures;

    public int Failures => Volatile.Read(ref _failures);

    public void Hold(int orderId)
    {
        lock (_held)
        {
            _held.Add(orderId);
        }
    }

    public void Release(int orderId)
    {
        lock (_held)
        {
            _held.Remove(orderId);
        }
    }

    public void ThrowIfHeld(int orderId)
    {
        lock (_held)
        {
            if (!_held.Contains(orderId))
            {
                return;
            }
        }

        Interlocked.Increment(ref _failures);
        throw new InvalidOperationException($"Order {orderId} is held.");
    }

    public async Task WaitForFailuresAsync(int count, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (Failures < count)
        {
            await Task.Delay(50, cts.Token);
        }
    }
}

public sealed class RecordingHandler(Journal journal, FailureGate gate) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        gate.ThrowIfHeld(message.OrderId);
        journal.Add(message.OrderId, context.PartitionKey);
        return Task.CompletedTask;
    }
}

public sealed class RejectingHandler : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken) =>
        throw new PermanentDeliveryException($"Order {message.OrderId} can never be handled.");
}
