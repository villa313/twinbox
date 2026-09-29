using Twinbox.Transport;

namespace Twinbox.RabbitMQ.Tests;

[MessageName("order-placed")]
public sealed record OrderPlaced(int OrderId);

public sealed class Journal
{
    private readonly List<int> _handled = [];

    public IReadOnlyList<int> Handled
    {
        get
        {
            lock (_handled)
            {
                return [.. _handled];
            }
        }
    }

    public void Add(int orderId)
    {
        lock (_handled)
        {
            _handled.Add(orderId);
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

public sealed class RecordingHandler(Journal journal) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        journal.Add(message.OrderId);
        return Task.CompletedTask;
    }
}

public sealed class RejectingHandler : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken) =>
        throw new PermanentDeliveryException($"Order {message.OrderId} can never be handled.");
}

public sealed class FailFirstAttemptHandler(Journal journal) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        if (context.DeliveryAttempt == 1)
        {
            throw new InvalidOperationException("first attempt fails");
        }

        journal.Add(context.DeliveryAttempt);
        return Task.CompletedTask;
    }
}
