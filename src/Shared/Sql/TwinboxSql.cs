using System.Globalization;
using Twinbox.Storage;

namespace Twinbox.Sql;

/// <summary>Statements for the operations LINQ can't express (SKIP LOCKED claims, conditional inserts), shared as source by
/// the relational stores. Callers pass delimited identifiers, so any naming convention works.</summary>
internal sealed partial class TwinboxSql
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

    /// <summary>False for MySQL, which has no UPDATE ... RETURNING: its claim is <see cref="LockDue"/>, <see cref="Lease"/>
    /// and <see cref="SelectLeased"/> in one transaction.</summary>
    public bool ClaimsInOneStatement => _provider != SqlProvider.MySql;

    /// <summary>Leases due rows; only the oldest unsent row of a partition is due.</summary>
    public string Claim()
    {
        var due = Due();
        return _provider switch
        {
            SqlProvider.Oracle => OracleClaim(due),
            SqlProvider.SqlServer => $"""
                WITH due AS (
                    SELECT TOP (@batch) * FROM {_outbox} AS o WITH (UPDLOCK, READPAST, ROWLOCK)
                    WHERE {due}
                    ORDER BY o.{Sequence})
                UPDATE due SET {_o("Status")} = {Processing}, {_o("LeaseOwner")} = @owner, {_o("LeaseUntil")} = @leaseUntil
                OUTPUT inserted.*;
                """,

            // SQLite serializes writers, so a plain UPDATE ... RETURNING is already exclusive.
            SqlProvider.Sqlite => $"""
                UPDATE {_outbox} SET {_o("Status")} = {Processing}, {_o("LeaseOwner")} = @owner, {_o("LeaseUntil")} = @leaseUntil
                WHERE {Sequence} IN (
                    SELECT o.{Sequence} FROM {_outbox} AS o
                    WHERE {due}
                    ORDER BY o.{Sequence}
                    LIMIT @batch)
                RETURNING *;
                """,
            SqlProvider.MySql => throw new NotSupportedException("MySQL claims in several statements, starting with LockDue."),
            _ => $"""
                WITH due AS (
                    SELECT o.{Sequence} FROM {_outbox} AS o
                    WHERE {due}
                    ORDER BY o.{Sequence}
                    LIMIT @batch
                    FOR UPDATE SKIP LOCKED)
                UPDATE {_outbox} AS t SET {_o("Status")} = {Processing}, {_o("LeaseOwner")} = @owner, {_o("LeaseUntil")} = @leaseUntil
                FROM due WHERE t.{Sequence} = due.{Sequence}
                RETURNING t.*;
                """,
        };
    }

    /// <summary>Locks the keys of due rows; the caller leases and reads them back before committing.</summary>
    public string LockDue() => $"""
        SELECT o.{Sequence} FROM {_outbox} AS o
        WHERE {Due()}
        ORDER BY o.{Sequence}
        LIMIT @batch
        FOR UPDATE SKIP LOCKED;
        """;

    public string Lease(IEnumerable<long> sequences) => $"""
        UPDATE {_outbox} SET {_o("Status")} = {Processing}, {_o("LeaseOwner")} = @owner, {_o("LeaseUntil")} = @leaseUntil
        WHERE {Sequence} IN ({KeyList(sequences)});
        """;

    public string SelectLeased(IEnumerable<long> sequences) =>
        $"SELECT * FROM {_outbox} WHERE {Sequence} IN ({KeyList(sequences)}) ORDER BY {Sequence}";

    /// <summary>One statement per outcome; <paramref name="index"/> keeps parameter names unique within a batch.</summary>
    public string Complete(int index) => _provider == SqlProvider.Oracle ? OracleComplete(index) : $"""
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

    /// <summary>Joins <see cref="Complete"/> statements into one command text.</summary>
    public string Batch(string statements) => _provider == SqlProvider.Oracle ? OracleBlock(statements) : statements;

    public string PurgeOutbox(bool includeDead)
    {
        var expired = $"({_o("Status")} = {Sent} AND {_o("SentAt")} < @sentBefore)"
            + (includeDead ? $" OR ({_o("Status")} = {Dead} AND {_o("CreatedAt")} < @deadBefore)" : string.Empty);

        return _provider switch
        {
            SqlProvider.Oracle => OraclePurgeOutbox(expired),
            SqlProvider.SqlServer => $"DELETE TOP (@batch) FROM {_outbox} WITH (READPAST) WHERE {expired};",
            SqlProvider.Sqlite => $"""
                DELETE FROM {_outbox} WHERE {Sequence} IN (
                    SELECT {Sequence} FROM {_outbox} WHERE {expired} LIMIT @batch);
                """,
            SqlProvider.MySql => $"DELETE FROM {_outbox} WHERE {expired} ORDER BY {Sequence} LIMIT @batch;",
            _ => $"""
                DELETE FROM {_outbox} WHERE {Sequence} IN (
                    SELECT {Sequence} FROM {_outbox} WHERE {expired} LIMIT @batch FOR UPDATE SKIP LOCKED);
                """,
        };
    }

    /// <summary>Inserts the entry unless it exists; a concurrent duplicate waits for the first to commit or roll back.</summary>
    public string InsertInbox()
    {
        var columns = $"{_i("MessageId")}, {_i("Consumer")}, {_i("Source")}, {_i("ProcessedAt")}";
        return _provider switch
        {
            SqlProvider.Oracle => OracleInsertInbox(columns),
            // A HOLDLOCK existence check range-locks the gap after the newest key, serializing all new messages; a plain insert
            // blocks only true duplicates. XACT_ABORT is off around it so a caught duplicate-key error can't doom the transaction.
            SqlProvider.SqlServer => $"""
                DECLARE @xactAbort int = @@OPTIONS & 16384;
                SET XACT_ABORT OFF;
                BEGIN TRY
                    INSERT INTO {_inbox} ({columns}) VALUES (@messageId, @consumer, @source, @processedAt);
                END TRY
                BEGIN CATCH
                    IF @xactAbort <> 0 SET XACT_ABORT ON;
                    IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
                END CATCH;
                IF @xactAbort <> 0 SET XACT_ABORT ON;
                """,
            SqlProvider.Sqlite => $"INSERT OR IGNORE INTO {_inbox} ({columns}) VALUES (@messageId, @consumer, @source, @processedAt);",

            // ON DUPLICATE KEY UPDATE reports 1 row under the drivers' default found-rows mode; IGNORE reports 0.
            SqlProvider.MySql => $"INSERT IGNORE INTO {_inbox} ({columns}) VALUES (@messageId, @consumer, @source, @processedAt);",
            _ => $"INSERT INTO {_inbox} ({columns}) VALUES (@messageId, @consumer, @source, @processedAt) ON CONFLICT DO NOTHING;",
        };
    }

    public string PurgeInbox() => _provider switch
    {
        SqlProvider.Oracle => OraclePurgeInbox(),
        SqlProvider.SqlServer => $"DELETE TOP (@batch) FROM {_inbox} WITH (READPAST) WHERE {_i("ProcessedAt")} < @before;",
        SqlProvider.Sqlite => $"""
            DELETE FROM {_inbox} WHERE rowid IN (
                SELECT rowid FROM {_inbox} WHERE {_i("ProcessedAt")} < @before LIMIT @batch);
            """,
        SqlProvider.MySql => $"""
            DELETE FROM {_inbox} WHERE {_i("ProcessedAt")} < @before
            ORDER BY {_i("ProcessedAt")}, {_i("MessageId")}, {_i("Consumer")} LIMIT @batch;
            """,
        _ => $"""
            DELETE FROM {_inbox} WHERE ({_i("MessageId")}, {_i("Consumer")}) IN (
                SELECT {_i("MessageId")}, {_i("Consumer")} FROM {_inbox}
                WHERE {_i("ProcessedAt")} < @before LIMIT @batch FOR UPDATE SKIP LOCKED);
            """,
    };

    private static string KeyList(IEnumerable<long> sequences) =>
        string.Join(", ", sequences.Select(s => s.ToString(CultureInfo.InvariantCulture)));

    private string Due() => $"""
        ((o.{_o("Status")} = {Pending} AND o.{_o("AvailableAt")} <= @now)
            OR (o.{_o("Status")} = {Processing} AND o.{_o("LeaseUntil")} < @now))
        AND (o.{_o("PartitionKey")} IS NULL OR NOT EXISTS (
            SELECT 1 FROM {_outbox} p
            WHERE p.{_o("PartitionKey")} = o.{_o("PartitionKey")}
              AND p.{_o("Status")} IN ({Pending}, {Processing})
              AND p.{Sequence} < o.{Sequence}))
        """;
}
