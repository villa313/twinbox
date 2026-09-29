using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore.Sql;

/// <summary>
/// Hand-written statements for the operations LINQ can't express (SKIP LOCKED claims, conditional inserts).
/// Identifiers come from the model, so naming conventions and custom table names are honoured.
/// </summary>
internal sealed class TwinboxSql
{
    private const int Pending = (int)OutboxMessageStatus.Pending;
    private const int Processing = (int)OutboxMessageStatus.Processing;
    private const int Sent = (int)OutboxMessageStatus.Sent;
    private const int Dead = (int)OutboxMessageStatus.Dead;

    private static readonly ConditionalWeakTable<IModel, TwinboxSql> Cache = [];

    private readonly SqlProvider _provider;
    private readonly string _outbox;
    private readonly string _inbox;
    private readonly Func<string, string> _o;
    private readonly Func<string, string> _i;

    private TwinboxSql(DbContext context)
    {
        _provider = context.Database.ProviderName switch
        {
            "Microsoft.EntityFrameworkCore.SqlServer" => SqlProvider.SqlServer,
            "Npgsql.EntityFrameworkCore.PostgreSQL" => SqlProvider.PostgreSql,
            var other => throw new NotSupportedException(
                $"Twinbox.EntityFrameworkCore supports SQL Server and PostgreSQL; '{other}' is not supported yet."),
        };

        var helper = context.GetService<ISqlGenerationHelper>();
        var outboxType = FindEntityType(context, typeof(OutboxMessage));
        var inboxType = FindEntityType(context, typeof(InboxRecord));
        _outbox = helper.DelimitIdentifier(outboxType.GetTableName()!, outboxType.GetSchema());
        _inbox = helper.DelimitIdentifier(inboxType.GetTableName()!, inboxType.GetSchema());
        _o = ColumnResolver(helper, outboxType);
        _i = ColumnResolver(helper, inboxType);
    }

    public static TwinboxSql For(DbContext context) =>
        Cache.GetValue(context.Model, _ => new TwinboxSql(context));

    public string Sequence => _o(TwinboxModelBuilderExtensions.SequenceProperty);

    /// <summary>Leases due rows; only the oldest unsent row of a partition is due.</summary>
    public string Claim()
    {
        var due = $"""
            ((o.{_o("Status")} = {Pending} AND o.{_o("AvailableAt")} <= @now)
                OR (o.{_o("Status")} = {Processing} AND o.{_o("LeaseUntil")} < @now))
            AND (o.{_o("PartitionKey")} IS NULL OR NOT EXISTS (
                SELECT 1 FROM {_outbox} p
                WHERE p.{_o("PartitionKey")} = o.{_o("PartitionKey")}
                  AND p.{_o("Status")} IN ({Pending}, {Processing})
                  AND p.{Sequence} < o.{Sequence}))
            """;

        return _provider == SqlProvider.SqlServer
            ? $"""
                WITH due AS (
                    SELECT TOP (@batch) * FROM {_outbox} AS o WITH (UPDLOCK, READPAST, ROWLOCK)
                    WHERE {due}
                    ORDER BY o.{Sequence})
                UPDATE due SET {_o("Status")} = {Processing}, {_o("LeaseOwner")} = @owner, {_o("LeaseUntil")} = @leaseUntil
                OUTPUT inserted.*;
                """
            : $"""
                WITH due AS (
                    SELECT o.{Sequence} FROM {_outbox} AS o
                    WHERE {due}
                    ORDER BY o.{Sequence}
                    LIMIT @batch
                    FOR UPDATE SKIP LOCKED)
                UPDATE {_outbox} AS t SET {_o("Status")} = {Processing}, {_o("LeaseOwner")} = @owner, {_o("LeaseUntil")} = @leaseUntil
                FROM due WHERE t.{Sequence} = due.{Sequence}
                RETURNING t.*;
                """;
    }

    /// <summary>One statement per outcome; <paramref name="index"/> keeps parameter names unique within a batch.</summary>
    public string Complete(int index) => $"""
        UPDATE {_outbox} SET
            {_o("Status")} = @status{index},
            {_o("Attempts")} = @attempts{index},
            {_o("AvailableAt")} = COALESCE(@availableAt{index}, {_o("AvailableAt")}),
            {_o("SentAt")} = @sentAt{index},
            {_o("LastError")} = COALESCE(@error{index}, {_o("LastError")}),
            {_o("LeaseOwner")} = NULL,
            {_o("LeaseUntil")} = NULL
        WHERE {_o("Id")} = @id{index} AND {_o("LeaseOwner")} = @owner AND {_o("Status")} = {Processing};
        """;

    public string PurgeOutbox(bool includeDead)
    {
        var expired = $"({_o("Status")} = {Sent} AND {_o("SentAt")} < @sentBefore)"
            + (includeDead ? $" OR ({_o("Status")} = {Dead} AND {_o("CreatedAt")} < @deadBefore)" : string.Empty);

        return _provider == SqlProvider.SqlServer
            ? $"DELETE TOP (@batch) FROM {_outbox} WITH (READPAST) WHERE {expired};"
            : $"""
                DELETE FROM {_outbox} WHERE {Sequence} IN (
                    SELECT {Sequence} FROM {_outbox} WHERE {expired} LIMIT @batch FOR UPDATE SKIP LOCKED);
                """;
    }

    /// <summary>Inserts the entry unless it exists; a concurrent duplicate waits for the first to commit or roll back.</summary>
    public string InsertInbox()
    {
        var columns = $"{_i("MessageId")}, {_i("Consumer")}, {_i("Source")}, {_i("ProcessedAt")}";
        return _provider == SqlProvider.SqlServer
            ? $"""
                INSERT INTO {_inbox} ({columns})
                SELECT @messageId, @consumer, @source, @processedAt
                WHERE NOT EXISTS (
                    SELECT 1 FROM {_inbox} WITH (UPDLOCK, HOLDLOCK)
                    WHERE {_i("MessageId")} = @messageId AND {_i("Consumer")} = @consumer);
                """
            : $"INSERT INTO {_inbox} ({columns}) VALUES (@messageId, @consumer, @source, @processedAt) ON CONFLICT DO NOTHING;";
    }

    public string PurgeInbox() =>
        _provider == SqlProvider.SqlServer
            ? $"DELETE TOP (@batch) FROM {_inbox} WITH (READPAST) WHERE {_i("ProcessedAt")} < @before;"
            : $"""
                DELETE FROM {_inbox} WHERE ({_i("MessageId")}, {_i("Consumer")}) IN (
                    SELECT {_i("MessageId")}, {_i("Consumer")} FROM {_inbox}
                    WHERE {_i("ProcessedAt")} < @before LIMIT @batch FOR UPDATE SKIP LOCKED);
                """;

    private static IEntityType FindEntityType(DbContext context, Type type) =>
        context.Model.FindEntityType(type)
            ?? throw new InvalidOperationException(
                $"{context.GetType().Name} has no Twinbox tables. Call modelBuilder.AddTwinbox() in OnModelCreating and add a migration.");

    private static Func<string, string> ColumnResolver(ISqlGenerationHelper helper, IEntityType entityType)
    {
        var table = StoreObjectIdentifier.Table(entityType.GetTableName()!, entityType.GetSchema());
        return property => helper.DelimitIdentifier(entityType.FindProperty(property)!.GetColumnName(table)!);
    }
}
