using Microsoft.Extensions.Options;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.Maintenance;

internal sealed class TwinboxMaintenance(
    IOutboxDispatcher dispatcher,
    IEnumerable<IOutboxStore> outboxes,
    TenantDirectory tenants,
    IOptions<TwinboxOptions> options,
    TimeProvider time,
    IInboxStore? inbox = null) : ITwinboxMaintenance
{
    public async Task<int> DispatchPendingAsync(TimeSpan budget, CancellationToken cancellationToken)
    {
        var unlimited = budget == Timeout.InfiniteTimeSpan;
        if (!unlimited)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(budget, TimeSpan.Zero);
        }

        var started = time.GetTimestamp();
        var total = 0;
        while (!cancellationToken.IsCancellationRequested && (unlimited || time.GetElapsedTime(started) < budget))
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

    public async Task<int> RunCleanupAsync(CancellationToken cancellationToken)
    {
        var retention = options.Value.Retention;
        var removed = 0;
        await tenants.ForEachTenantAsync(
            async ct => removed += await PurgeTenantAsync(retention, ct).ConfigureAwait(false),
            cancellationToken).ConfigureAwait(false);
        return removed;
    }

    private async Task<int> PurgeTenantAsync(RetentionOptions retention, CancellationToken cancellationToken)
    {
        var now = time.GetUtcNow();
        var purge = new OutboxPurge(now - retention.SentMessages, now - retention.DeadMessages, retention.BatchSize);
        var removed = 0;
        foreach (var outbox in outboxes)
        {
            int purged;
            do
            {
                purged = await outbox.PurgeAsync(purge, cancellationToken).ConfigureAwait(false);
                removed += purged;
            }
            while (purged >= retention.BatchSize);
        }

        if (inbox is null)
        {
            return removed;
        }

        var processedBefore = now - retention.InboxEntries;
        int expired;
        do
        {
            expired = await inbox.PurgeAsync(processedBefore, retention.BatchSize, cancellationToken).ConfigureAwait(false);
            removed += expired;
        }
        while (expired >= retention.BatchSize);

        return removed;
    }
}
