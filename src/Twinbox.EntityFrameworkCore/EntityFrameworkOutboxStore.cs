using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.EntityFrameworkCore.Sql;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore;

internal sealed class EntityFrameworkOutboxStore<TContext>(IServiceScopeFactory scopeFactory) : IOutboxStore
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
            context.Set<OutboxMessage>().AddRange(messages);
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
            var sql = TwinboxSql.For(context);
            var rows = await context.Set<OutboxMessage>()
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
            var sql = TwinboxSql.For(context);
            foreach (var chunk in outcomes.Chunk(OutcomesPerCommand))
            {
                var parameters = new List<DbParameter> { Parameters.Create(context, "@owner", owner, DbType.String) };
                var statements = new System.Text.StringBuilder();
                for (var i = 0; i < chunk.Length; i++)
                {
                    statements.AppendLine(sql.Complete(i));
                    parameters.AddRange(OutcomeParameters(context, chunk[i], i));
                }

                await context.Database.ExecuteSqlRawAsync(statements.ToString(), parameters, cancellationToken).ConfigureAwait(false);
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
                .ExecuteSqlRawAsync(TwinboxSql.For(context).PurgeOutbox(purge.DeadBefore is not null), parameters, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var messages = scope.ServiceProvider.GetRequiredService<TContext>().Set<OutboxMessage>().AsNoTracking();
            var unsent = messages.Where(m => m.Status == OutboxMessageStatus.Pending || m.Status == OutboxMessageStatus.Processing);
            var pendingCount = await unsent.LongCountAsync(cancellationToken).ConfigureAwait(false);
            var oldest = pendingCount == 0
                ? null
                : await unsent.MinAsync(m => (DateTimeOffset?)m.CreatedAt, cancellationToken).ConfigureAwait(false);
            var deadCount = await messages.LongCountAsync(m => m.Status == OutboxMessageStatus.Dead, cancellationToken).ConfigureAwait(false);
            return new OutboxStatistics(pendingCount, oldest, deadCount);
        }
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
