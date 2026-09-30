using System.Data;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Sql;
using Twinbox.Storage;

namespace Twinbox.Relational;

internal sealed class RelationalInboxStore(RelationalDialect dialect, RelationalOutboxStore store, IDispatchSignal signal)
    : IInboxStore, IBatchInboxStore
{
    public async Task<bool> TryProcessAsync(
        InboxEntry entry,
        IServiceProvider scopedServices,
        Func<CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(handler);
        return await ProcessAsync([entry], scopedServices, (_, ct) => handler(ct), cancellationToken).ConfigureAwait(false) == 1;
    }

    public Task<int> TryProcessBatchAsync(
        IReadOnlyList<InboxEntry> entries,
        IServiceProvider scopedServices,
        Func<IReadOnlyList<int>, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(handler);
        return ProcessAsync(entries, scopedServices, handler, cancellationToken);
    }

    public async Task<int> PurgeAsync(DateTimeOffset processedBefore, int batchSize, CancellationToken cancellationToken)
    {
        await using var lease = await store.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = lease.Connection.Command(dialect.Sql.PurgeInbox())
            .With("@before", processedBefore, DbType.DateTimeOffset)
            .With("@batch", batchSize, DbType.Int32);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<int> ProcessAsync(
        IReadOnlyList<InboxEntry> entries,
        IServiceProvider scopedServices,
        Func<IReadOnlyList<int>, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(scopedServices);
        var session = scopedServices.GetRequiredService<IOutboxSession>();
        var handlerTransaction = scopedServices.GetRequiredService<HandlerTransaction>();

        await using var lease = await store.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await lease.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var fresh = new List<int>();
        foreach (var i in InboxLockOrder.Of(entries))
        {
            await using var insert = lease.Connection.Command(dialect.Sql.InsertInbox(), transaction)
                .With("@messageId", entries[i].MessageId, DbType.String)
                .With("@consumer", entries[i].Consumer, DbType.String)
                .With("@source", entries[i].Source, DbType.String)
                .With("@processedAt", entries[i].ReceivedAt, DbType.DateTimeOffset);
            if (await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1)
            {
                fresh.Add(i);
            }
        }

        if (fresh.Count == 0)
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return 0;
        }

        fresh.Sort();

        using var binding = HandlerTransactionBinding.Bind(handlerTransaction, lease.Connection, transaction);
        try
        {
            await handler(fresh, cancellationToken).ConfigureAwait(false);
            var pending = session.TakePending();
            await RelationalOutboxStore.InsertAsync(dialect, transaction, pending, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            if (pending.Count > 0)
            {
                signal.Notify();
            }

            return fresh.Count;
        }
        catch
        {
            session.TakePending();
            throw;
        }
    }
}
