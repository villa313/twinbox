using System.Data;
using System.Data.Common;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.Relational;

internal sealed class RelationalOutboxStore(
    RelationalSettings settings,
    RelationalDialect dialect,
    SchemaInitializer schema,
    TwinboxScopeFactory scopes) : IOutboxStore
{
    // Keeps each command well under SQL Server's 2100-parameter limit.
    private const int RowsPerInsert = 100;
    private const int OutcomesPerCommand = 250;

    public async Task AppendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await lease.Connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await InsertAsync(dialect, transaction, messages, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<OutboxMessage>> ClaimAsync(OutboxClaim claim, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(claim);
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        if (!dialect.Sql.ClaimsInOneStatement)
        {
            return await ClaimInTransactionAsync(lease.Connection, claim, cancellationToken).ConfigureAwait(false);
        }

        await using var command = lease.Connection.Command(dialect.Sql.Claim())
            .With("@now", claim.Now, DbType.DateTimeOffset)
            .With("@batch", claim.BatchSize, DbType.Int32)
            .With("@owner", claim.Owner, DbType.String)
            .With("@leaseUntil", claim.Now + claim.LeaseDuration, DbType.DateTimeOffset);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await OutboxRowReader.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
    }

    public async Task CompleteAsync(string owner, IReadOnlyList<DispatchOutcome> outcomes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(outcomes);
        if (outcomes.Count == 0)
        {
            return;
        }

        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        foreach (var chunk in outcomes.Chunk(OutcomesPerCommand))
        {
            await using var command = lease.Connection.Command(dialect.Sql.Batch(string.Join('\n', chunk.Select((_, i) => dialect.Sql.Complete(i)))))
                .With("@owner", owner, DbType.String);
            for (var i = 0; i < chunk.Length; i++)
            {
                command.With($"@status{i}", (int)chunk[i].Status, DbType.Int32)
                    .With($"@attempts{i}", chunk[i].Attempts, DbType.Int32)
                    .With($"@availableAt{i}", chunk[i].AvailableAt, DbType.DateTimeOffset)
                    .With($"@sentAt{i}", chunk[i].SentAt, DbType.DateTimeOffset)
                    .With($"@error{i}", chunk[i].Error, DbType.String)
                    .With($"@id{i}", chunk[i].MessageId, DbType.Guid);
            }

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<int> PurgeAsync(OutboxPurge purge, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(purge);
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = lease.Connection.Command(dialect.Sql.PurgeOutbox(purge.DeadBefore is not null))
            .With("@sentBefore", purge.SentBefore, DbType.DateTimeOffset)
            .With("@batch", purge.BatchSize, DbType.Int32);
        if (purge.DeadBefore is { } deadBefore)
        {
            command.With("@deadBefore", deadBefore, DbType.DateTimeOffset);
        }

        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = lease.Connection.Command(dialect.Statistics());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return new OutboxStatistics(
            Convert.ToInt64(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<DateTimeOffset>(1),
            Convert.ToInt64(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture));
    }

    internal static async Task InsertAsync(
        RelationalDialect dialect,
        DbTransaction transaction,
        IReadOnlyList<OutboxMessage> messages,
        CancellationToken cancellationToken)
    {
        foreach (var chunk in messages.Chunk(RowsPerInsert))
        {
            await using var command = transaction.Connection!.Command(dialect.Insert(chunk.Length), transaction);
            for (var row = 0; row < chunk.Length; row++)
            {
                AddRow(command, chunk[row], row);
            }

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task<ConnectionLease> OpenAsync(CancellationToken cancellationToken)
    {
        var scope = scopes.CreateAsyncScope();
        var connection = settings.CreateConnection(scope.ServiceProvider);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await schema.EnsureAsync(connection, cancellationToken).ConfigureAwait(false);
            return new ConnectionLease(scope, connection);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            await scope.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task<IReadOnlyList<OutboxMessage>> ClaimInTransactionAsync(
        DbConnection connection,
        OutboxClaim claim,
        CancellationToken cancellationToken)
    {
        // Read committed stops the locking read from taking gap locks that would stall concurrent appends.
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
        var sequences = new List<long>();
        await using (var select = connection.Command(dialect.Sql.LockDue(), transaction)
            .With("@now", claim.Now, DbType.DateTimeOffset)
            .With("@batch", claim.BatchSize, DbType.Int32))
        await using (var keys = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await keys.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                sequences.Add(keys.GetInt64(0));
            }
        }

        IReadOnlyList<OutboxMessage> leased = [];
        if (sequences.Count > 0)
        {
            await using var command = connection.Command(dialect.Sql.Lease(sequences) + dialect.Sql.SelectLeased(sequences), transaction)
                .With("@owner", claim.Owner, DbType.String)
                .With("@leaseUntil", claim.Now + claim.LeaseDuration, DbType.DateTimeOffset);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            leased = await OutboxRowReader.ReadAsync(reader, cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return leased;
    }

    private static void AddRow(DbCommand command, OutboxMessage m, int row) => command
        .With($"@Id{row}", m.Id, DbType.Guid)
        .With($"@MessageName{row}", m.MessageName, DbType.String)
        .With($"@Transport{row}", m.Transport, DbType.String)
        .With($"@Destination{row}", m.Destination, DbType.String)
        .With($"@PartitionKey{row}", m.PartitionKey, DbType.String)
        .With($"@TenantId{row}", m.TenantId, DbType.String)
        .With($"@Payload{row}", m.Payload, DbType.Binary)
        .With($"@ContentType{row}", m.ContentType, DbType.String)
        .With($"@Headers{row}", HeaderCodec.Encode(m.Headers) ?? "{}", DbType.String)
        .With($"@TraceParent{row}", m.TraceParent, DbType.String)
        .With($"@CreatedAt{row}", m.CreatedAt, DbType.DateTimeOffset)
        .With($"@AvailableAt{row}", m.AvailableAt, DbType.DateTimeOffset)
        .With($"@Attempts{row}", m.Attempts, DbType.Int32)
        .With($"@Status{row}", (int)m.Status, DbType.Int32)
        .With($"@LeaseOwner{row}", m.LeaseOwner, DbType.String)
        .With($"@LeaseUntil{row}", m.LeaseUntil, DbType.DateTimeOffset)
        .With($"@LastError{row}", m.LastError, DbType.String)
        .With($"@SentAt{row}", m.SentAt, DbType.DateTimeOffset);

    internal sealed class ConnectionLease(Microsoft.Extensions.DependencyInjection.AsyncServiceScope scope, DbConnection connection) : IAsyncDisposable
    {
        public DbConnection Connection => connection;

        public IServiceProvider Services => scope.ServiceProvider;

        public async ValueTask DisposeAsync()
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            await scope.DisposeAsync().ConfigureAwait(false);
        }
    }
}
