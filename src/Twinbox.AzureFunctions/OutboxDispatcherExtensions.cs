namespace Twinbox;

public static class OutboxDispatcherExtensions
{
    /// <summary>Leaves headroom under the Consumption plan's shortest function timeout.</summary>
    public static readonly TimeSpan DefaultDispatchBudget = TimeSpan.FromSeconds(50);

    /// <summary>Dispatches batches until the outbox is empty or <see cref="DefaultDispatchBudget"/> is used up; returns messages claimed.</summary>
    public static Task<int> DispatchPendingAsync(this IOutboxDispatcher dispatcher, CancellationToken cancellationToken) =>
        dispatcher.DispatchPendingAsync(DefaultDispatchBudget, TimeProvider.System, cancellationToken);

    /// <summary>Dispatches batches until the outbox is empty or the budget is used up; a batch already started is finished.</summary>
    public static Task<int> DispatchPendingAsync(this IOutboxDispatcher dispatcher, TimeSpan budget, CancellationToken cancellationToken) =>
        dispatcher.DispatchPendingAsync(budget, TimeProvider.System, cancellationToken);

    internal static async Task<int> DispatchPendingAsync(
        this IOutboxDispatcher dispatcher,
        TimeSpan budget,
        TimeProvider time,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dispatcher);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(budget, TimeSpan.Zero);

        var started = time.GetTimestamp();
        var total = 0;
        while (!cancellationToken.IsCancellationRequested && time.GetElapsedTime(started) < budget)
        {
            var claimed = await dispatcher.DispatchBatchAsync(cancellationToken).ConfigureAwait(false);
            if (claimed == 0)
            {
                break;
            }

            total += claimed;
        }

        return total;
    }
}
