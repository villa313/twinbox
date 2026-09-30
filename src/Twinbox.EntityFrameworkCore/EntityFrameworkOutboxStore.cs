using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.EntityFrameworkCore.Sql;
using Twinbox.Sql;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.EntityFrameworkCore;

internal sealed class EntityFrameworkOutboxStore<TContext>(TwinboxScopeFactory scopeFactory) : IOutboxStore, IOutboxAdmin
    where TContext : DbContext
{
    // Keeps each Complete batch well under SQL Server's 2100-parameter limit.
    private const int OutcomesPerCommand = 250;

    // Ids go in as individual parameters: collection parameters aren't translated the same way by every provider.
    private const int IdsPerCommand = 100;

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

    public async Task<OutboxPage> QueryAsync(OutboxQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(query.Take);
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var rows = Filter(scope.ServiceProvider.GetRequiredService<TContext>().TwinboxOutbox().AsNoTracking(), query);
            var matches = await rows
                .OrderByDescending(m => EF.Property<long>(m, TwinboxModelBuilderExtensions.SequenceProperty))
                .Take(query.Take + 1)
                .Select(m => new { Message = m, Sequence = EF.Property<long>(m, TwinboxModelBuilderExtensions.SequenceProperty) })
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);

            var page = matches.Take(query.Take).ToArray();
            var next = matches.Count > query.Take ? page[^1].Sequence.ToString(CultureInfo.InvariantCulture) : null;
            return new OutboxPage([.. page.Select(r => r.Message)], next);
        }
    }

    public async Task<OutboxMessage?> GetAsync(Guid id, CancellationToken cancellationToken)
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            return await scope.ServiceProvider.GetRequiredService<TContext>().TwinboxOutbox()
                .AsNoTracking()
                .FirstOrDefaultAsync(m => m.Id == id, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task<int> ReplayAsync(IReadOnlyCollection<Guid> ids, DateTimeOffset now, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(ids);
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var outbox = scope.ServiceProvider.GetRequiredService<TContext>().TwinboxOutbox();
            var changed = 0;
            foreach (var chunk in ids.Distinct().Chunk(IdsPerCommand))
            {
                changed += await outbox
                    .Where(IdIn(chunk))
                    .Where(m => m.Status == OutboxMessageStatus.Dead || m.Status == OutboxMessageStatus.Sent)
                    .ExecuteUpdateAsync(
                        s => s.SetProperty(m => m.Status, OutboxMessageStatus.Pending)
                            .SetProperty(m => m.Attempts, 0)
                            .SetProperty(m => m.AvailableAt, now)
                            .SetProperty(m => m.LastError, (string?)null)
                            .SetProperty(m => m.SentAt, (DateTimeOffset?)null)
                            .SetProperty(m => m.LeaseOwner, (string?)null)
                            .SetProperty(m => m.LeaseUntil, (DateTimeOffset?)null),
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            return changed;
        }
    }

    public async Task<int> DeleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(ids);
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var outbox = scope.ServiceProvider.GetRequiredService<TContext>().TwinboxOutbox();
            var deleted = 0;
            foreach (var chunk in ids.Distinct().Chunk(IdsPerCommand))
            {
                deleted += await outbox.Where(IdIn(chunk)).ExecuteDeleteAsync(cancellationToken).ConfigureAwait(false);
            }

            return deleted;
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

    private static IQueryable<OutboxMessage> Filter(IQueryable<OutboxMessage> rows, OutboxQuery query)
    {
        if (long.TryParse(query.Cursor, NumberStyles.None, CultureInfo.InvariantCulture, out var before))
        {
            rows = rows.Where(m => EF.Property<long>(m, TwinboxModelBuilderExtensions.SequenceProperty) < before);
        }

        if (query.Status is { } status)
        {
            rows = rows.Where(m => m.Status == status);
        }

        if (query.Destination is { } destination)
        {
            rows = rows.Where(m => m.Destination == destination);
        }

        if (query.MessageName is { } name)
        {
            rows = rows.Where(m => m.MessageName == name);
        }

        if (query.Search is { } search)
        {
            rows = Guid.TryParse(search, out var id)
                ? rows.Where(m => m.Id == id || m.PartitionKey == search)
                : rows.Where(m => m.PartitionKey == search);
        }

        return rows;
    }

    private static Expression<Func<OutboxMessage, bool>> IdIn(Guid[] ids)
    {
        var message = Expression.Parameter(typeof(OutboxMessage), "m");
        var id = Expression.Property(message, nameof(OutboxMessage.Id));
        var body = ids.Select(value => (Expression)Expression.Equal(id, Captured(value))).Aggregate(Expression.OrElse);
        return Expression.Lambda<Func<OutboxMessage, bool>>(body, message);
    }

    // A closure member rather than a constant, so EF sends each id as a parameter instead of inlining it.
    private static Expression Captured(Guid value)
    {
        Expression<Func<Guid>> capture = () => value;
        return capture.Body;
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
