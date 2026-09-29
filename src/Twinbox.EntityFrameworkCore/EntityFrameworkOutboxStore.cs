using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.EntityFrameworkCore.Sql;
using Twinbox.Sql;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.EntityFrameworkCore;

internal sealed class EntityFrameworkOutboxStore<TContext>(TwinboxScopeFactory scopeFactory) : IOutboxStore
    where TContext : DbContext
{
    // Keeps each Complete batch well under SQL Server's 2100-parameter limit.
    private const int OutcomesPerCommand = 250;

    public async Task AppendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var context = scope.ServiceProvider.GetRequiredService<TContext>();
            context.TwinboxOutbox().AddRange(messages);
            await context.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<IReadOnlyList<OutboxMessage>> ClaimAsync(OutboxClaim claim, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(claim);
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var context = scope.ServiceProvider.GetRequiredService<TContext>();
            var sql = EntityFrameworkSql.For(context);
            if (!sql.ClaimsInOneStatement)
            {
                return await ClaimInTransactionAsync(context, sql, claim, cancellationToken).ConfigureAwait(false);
            }

            var rows = await context.TwinboxOutbox()
                .FromSqlRaw(
                    sql.Claim(),
                    Parameters.Create(context, "@now", claim.Now, DbType.DateTimeOffset),
                    Parameters.Create(context, "@batch", claim.BatchSize, DbType.Int32),
                    Parameters.Create(context, "@owner", claim.Owner, DbType.String),
                    Parameters.Create(context, "@leaseUntil", claim.Now + claim.LeaseDuration, DbType.DateTimeOffset))
                .AsTracking()
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);

            // RETURNING/OUTPUT order is unspecified, so restore insertion order from the key.
            return [.. rows.OrderBy(m => context.Entry(m).Property<long>(TwinboxModelBuilderExtensions.SequenceProperty).CurrentValue)];
        }
    }

    public async Task CompleteAsync(string owner, IReadOnlyList<DispatchOutcome> outcomes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(outcomes);
        if (outcomes.Count == 0)
        {
            return;
        }

        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var context = scope.ServiceProvider.GetRequiredService<TContext>();
            var sql = EntityFrameworkSql.For(context);
            foreach (var chunk in outcomes.Chunk(OutcomesPerCommand))
            {
                var parameters = new List<DbParameter> { Parameters.Create(context, "@owner", owner, DbType.String) };
                var statements = new System.Text.StringBuilder();
                for (var i = 0; i < chunk.Length; i++)
                {
                    statements.AppendLine(sql.Complete(i));
                    parameters.AddRange(OutcomeParameters(context, chunk[i], i));
                }

                await context.Database.ExecuteSqlRawAsync(sql.Batch(statements.ToString()), parameters, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task<int> PurgeAsync(OutboxPurge purge, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(purge);
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var context = scope.ServiceProvider.GetRequiredService<TContext>();
            var parameters = new List<DbParameter>
            {
                Parameters.Create(context, "@sentBefore", purge.SentBefore, DbType.DateTimeOffset),
                Parameters.Create(context, "@batch", purge.BatchSize, DbType.Int32),
            };

            if (purge.DeadBefore is { } deadBefore)
            {
                parameters.Add(Parameters.Create(context, "@deadBefore", deadBefore, DbType.DateTimeOffset));
            }

            return await context.Database
                .ExecuteSqlRawAsync(EntityFrameworkSql.For(context).PurgeOutbox(purge.DeadBefore is not null), parameters, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var messages = scope.ServiceProvider.GetRequiredService<TContext>().TwinboxOutbox().AsNoTracking();
            var unsent = messages.Where(m => m.Status == OutboxMessageStatus.Pending || m.Status == OutboxMessageStatus.Processing);
            var pendingCount = await unsent.LongCountAsync(cancellationToken).ConfigureAwait(false);
            // Ordering by the key rather than MIN(CreatedAt), which SQLite can't evaluate on DateTimeOffset.
            var oldest = pendingCount == 0
                ? null
                : await unsent
                    .OrderBy(m => EF.Property<long>(m, TwinboxModelBuilderExtensions.SequenceProperty))
                    .Select(m => (DateTimeOffset?)m.CreatedAt)
                    .FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var deadCount = await messages.LongCountAsync(m => m.Status == OutboxMessageStatus.Dead, cancellationToken).ConfigureAwait(false);
            return new OutboxStatistics(pendingCount, oldest, deadCount);
        }
    }

    private static Task<IReadOnlyList<OutboxMessage>> ClaimInTransactionAsync(
        TContext context,
        TwinboxSql sql,
        OutboxClaim claim,
        CancellationToken cancellationToken) =>
        context.Database.CreateExecutionStrategy().ExecuteAsync(
            async ct =>
            {
                context.ChangeTracker.Clear();

                // Read committed stops the locking read from taking gap locks that would stall concurrent appends.
                var transaction = await context.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct).ConfigureAwait(false);
                await using (transaction.ConfigureAwait(false))
                {
                    var sequences = await LockDueAsync(context, sql, claim, transaction.GetDbTransaction(), ct).ConfigureAwait(false);
                    IReadOnlyList<OutboxMessage> leased = [];
                    if (sequences.Count > 0)
                    {
                        await context.Database.ExecuteSqlRawAsync(
                            sql.Lease(sequences),
                            [
                                Parameters.Create(context, "@owner", claim.Owner, DbType.String),
                                Parameters.Create(context, "@leaseUntil", claim.Now + claim.LeaseDuration, DbType.DateTimeOffset),
                            ],
                            ct).ConfigureAwait(false);
                        leased = await context.TwinboxOutbox()
                            .FromSqlRaw(sql.SelectLeased(sequences))
                            .AsTracking()
                            .ToListAsync(ct)
                            .ConfigureAwait(false);
                    }

                    await transaction.CommitAsync(ct).ConfigureAwait(false);
                    return leased;
                }
            },
            cancellationToken);

    private static async Task<List<long>> LockDueAsync(
        DbContext context,
        TwinboxSql sql,
        OutboxClaim claim,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = context.Database.GetDbConnection().CreateCommand();
        command.CommandText = sql.LockDue();
        command.Transaction = transaction;
        command.Parameters.Add(Parameters.Create(context, "@now", claim.Now, DbType.DateTimeOffset));
        command.Parameters.Add(Parameters.Create(context, "@batch", claim.BatchSize, DbType.Int32));

        var sequences = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            sequences.Add(reader.GetInt64(0));
        }

        return sequences;
    }

    private static IEnumerable<DbParameter> OutcomeParameters(DbContext context, DispatchOutcome outcome, int index) =>
    [
        Parameters.Create(context, $"@status{index}", (int)outcome.Status, DbType.Int32),
        Parameters.Create(context, $"@attempts{index}", outcome.Attempts, DbType.Int32),
        Parameters.Create(context, $"@availableAt{index}", outcome.AvailableAt, DbType.DateTimeOffset),
        Parameters.Create(context, $"@sentAt{index}", outcome.SentAt, DbType.DateTimeOffset),
        Parameters.Create(context, $"@error{index}", outcome.Error, DbType.String),
        Parameters.Create(context, $"@id{index}", outcome.MessageId, DbType.Guid),
    ];
}
