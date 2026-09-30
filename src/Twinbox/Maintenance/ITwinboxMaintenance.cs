namespace Twinbox;

/// <summary>Dispatch and cleanup on demand, for hosts that run them from a timer or job instead of the background services.</summary>
public interface ITwinboxMaintenance
{
    /// <summary>Dispatches until the outbox is empty or the budget is used up (<see cref="Timeout.InfiniteTimeSpan"/>: no budget); returns messages claimed.</summary>
    Task<int> DispatchPendingAsync(TimeSpan budget, CancellationToken cancellationToken);

    /// <summary>Purges per <see cref="RetentionOptions"/> for every tenant, even when retention is disabled; returns rows removed.</summary>
    Task<int> RunCleanupAsync(CancellationToken cancellationToken);
}
