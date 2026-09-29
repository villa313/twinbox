using System.Data;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.Relational;

internal sealed class RelationalInboxStore(RelationalDialect dialect, RelationalOutboxStore store, IDispatchSignal signal) : IInboxStore
{
    public async Task<bool> TryProcessAsync(
        InboxEntry entry,
        IServiceProvider scopedServices,
        Func<CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(scopedServices);
        ArgumentNullException.ThrowIfNull(handler);

        var session = scopedServices.GetRequiredService<IOutboxSession>();
        var handlerTransaction = scopedServices.GetRequiredService<HandlerTransaction>();

        await using var lease = await store.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await lease.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var insert = lease.Connection.Command(dialect.Sql.InsertInbox(), transaction)
            .With("@messageId", entry.MessageId, DbType.String)
            .With("@consumer", entry.Consumer, DbType.String)
            .With("@source", entry.Source, DbType.String)
            .With("@processedAt", entry.ReceivedAt, DbType.DateTimeOffset))
        {
            if (await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != 1)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }
        }

        handlerTransaction.Attach(lease.Connection, transaction);
        try
        {
            await handler(cancellationToken).ConfigureAwait(false);
            var pending = session.TakePending();
            await RelationalOutboxStore.InsertAsync(dialect, transaction, pending, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            if (pending.Count > 0)
            {
                signal.Notify();
            }

            return true;
        }
        catch
        {
            session.TakePending();
            throw;
        }
        finally
        {
            handlerTransaction.Detach();
        }
    }

    public async Task<int> PurgeAsync(DateTimeOffset processedBefore, int batchSize, CancellationToken cancellationToken)
    {
        await using var lease = await store.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = lease.Connection.Command(dialect.Sql.PurgeInbox())
            .With("@before", processedBefore, DbType.DateTimeOffset)
            .With("@batch", batchSize, DbType.Int32);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
