using Twinbox.Storage;

namespace Twinbox.Sql;

/// <summary>
/// Statements for the operations LINQ can't express (SKIP LOCKED claims, conditional inserts), shared as source by
/// the relational stores. Callers pass delimited identifiers, so any naming convention works.
/// </summary>
internal sealed class TwinboxSql
{
    private const int Pending = (int)OutboxMessageStatus.Pending;
    private const int Processing = (int)OutboxMessageStatus.Processing;
    private const int Sent = (int)OutboxMessageStatus.Sent;
    private const int Dead = (int)OutboxMessageStatus.Dead;

    private readonly SqlProvider _provider;
    private readonly string _outbox;
    private readonly string _inbox;
    private readonly Func<string, string> _o;
    private readonly Func<string, string> _i;

    /// <summary>The column resolvers map a property name (e.g. "AvailableAt") to its delimited column name.</summary>
    public TwinboxSql(
        SqlProvider provider,
        string outboxTable,
        string inboxTable,
        Func<string, string> outboxColumn,
        Func<string, string> inboxColumn)
    {
        _provider = provider;
        _outbox = outboxTable;
        _inbox = inboxTable;
        _o = outboxColumn;
        _i = inboxColumn;
    }

    public string Sequence => _o("Sequence");

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
}
