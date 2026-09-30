using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.EntityFrameworkCore.Sql;
using Twinbox.Sql;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.EntityFrameworkCore;

internal sealed class EntityFrameworkInboxStore<TContext>(TwinboxScopeFactory scopeFactory) : IInboxStore, IBatchInboxStore
    where TContext : DbContext
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

        var context = scopedServices.GetRequiredService<TContext>();
        var session = scopedServices.GetRequiredService<IOutboxSession>();
        var strategy = context.Database.CreateExecutionStrategy();

        return await strategy.ExecuteAsync(
            async ct =>
            {
                // A retried attempt must not see entities or messages left over from the failed one.
                context.ChangeTracker.Clear();
                session.TakePending();

                var transaction = await context.Database.BeginTransactionAsync(ct).ConfigureAwait(false);
                await using (transaction.ConfigureAwait(false))
                {
                    if (!await TryInsertAsync(context, entry, ct).ConfigureAwait(false))
                    {
                        await transaction.RollbackAsync(ct).ConfigureAwait(false);
                        return false;
                    }

                    await handler(ct).ConfigureAwait(false);
                    await context.SaveChangesAsync(ct).ConfigureAwait(false);
                    await transaction.CommitAsync(ct).ConfigureAwait(false);
                    return true;
                }
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> TryProcessBatchAsync(
        IReadOnlyList<InboxEntry> entries,
        IServiceProvider scopedServices,
        Func<IReadOnlyList<int>, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(scopedServices);
        ArgumentNullException.ThrowIfNull(handler);

        var context = scopedServices.GetRequiredService<TContext>();
        var session = scopedServices.GetRequiredService<IOutboxSession>();
        var strategy = context.Database.CreateExecutionStrategy();

        return await strategy.ExecuteAsync(
            async ct =>
            {
                context.ChangeTracker.Clear();
                session.TakePending();

                var transaction = await context.Database.BeginTransactionAsync(ct).ConfigureAwait(false);
                await using (transaction.ConfigureAwait(false))
                {
                    var fresh = new List<int>();
                    foreach (var i in InboxLockOrder.Of(entries))
                    {
                        if (await TryInsertAsync(context, entries[i], ct).ConfigureAwait(false))
                        {
                            fresh.Add(i);
                        }
                    }

                    if (fresh.Count == 0)
                    {
                        await transaction.RollbackAsync(ct).ConfigureAwait(false);
                        return 0;
                    }

                    fresh.Sort();

                    await handler(fresh, ct).ConfigureAwait(false);
                    await context.SaveChangesAsync(ct).ConfigureAwait(false);
                    await transaction.CommitAsync(ct).ConfigureAwait(false);
                    return fresh.Count;
                }
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> PurgeAsync(DateTimeOffset processedBefore, int batchSize, CancellationToken cancellationToken)
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var context = scope.ServiceProvider.GetRequiredService<TContext>();
            return await context.Database.ExecuteSqlRawAsync(
                EntityFrameworkSql.For(context).PurgeInbox(),
                [
                    Parameters.Create(context, "@before", processedBefore, DbType.DateTimeOffset),
                    Parameters.Create(context, "@batch", batchSize, DbType.Int32),
                ],
                cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task<bool> TryInsertAsync(DbContext context, InboxEntry entry, CancellationToken cancellationToken)
    {
        var inserted = await context.Database.ExecuteSqlRawAsync(
            EntityFrameworkSql.For(context).InsertInbox(),
            [
                Parameters.Create(context, "@messageId", entry.MessageId, DbType.String),
                Parameters.Create(context, "@consumer", entry.Consumer, DbType.String),
                Parameters.Create(context, "@source", entry.Source, DbType.String),
                Parameters.Create(context, "@processedAt", entry.ReceivedAt, DbType.DateTimeOffset),
            ],
            cancellationToken).ConfigureAwait(false);
        return inserted == 1;
    }
}
