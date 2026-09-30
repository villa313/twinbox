namespace Twinbox.AotSmoke;

public sealed class OrderPlacedHandler(Ledger ledger) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        if (message.Sku != $"SKU-{message.OrderId}" || message.Total != message.OrderId * 10.5m)
        {
            throw new InvalidOperationException($"Order {message.OrderId} arrived corrupted: {message}");
        }

        ledger.Record("order", message.OrderId, context.MessageId);
        return Task.CompletedTask;
    }
}

public sealed class InvoiceBatchHandler(Ledger ledger) : IHandleBatch<InvoiceRequested>
{
    public Task HandleAsync(IReadOnlyList<BatchItem<InvoiceRequested>> batch, CancellationToken cancellationToken)
    {
        foreach (var item in batch)
        {
            ledger.Record("invoice", item.Message.OrderId, item.Context.MessageId);
        }

        return Task.CompletedTask;
    }
}

public sealed class CountingFilter(Ledger ledger) : IMessageFilter
{
    public async Task InvokeAsync(object message, MessageContext context, Func<Task> continuation, CancellationToken cancellationToken)
    {
        await continuation().ConfigureAwait(false);
        ledger.RecordFilter();
    }
}

public sealed class CountingBatchFilter(Ledger ledger) : IBatchMessageFilter
{
    public async Task InvokeAsync(IReadOnlyList<BatchItem<object>> batch, Func<Task> continuation, CancellationToken cancellationToken)
    {
        await continuation().ConfigureAwait(false);
        ledger.RecordBatchFilter(batch.Count);
    }
}
