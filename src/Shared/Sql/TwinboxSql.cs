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
    private static readonly string[] OutcomeColumns = ["id", "status", "attempts", "available_at", "sent_at", "error"];

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

    /// <summary>One set-based UPDATE for <paramref name="count"/> outcomes, bound as @id0, @status0 and so on.</summary>
    public string Complete(int count)
    {
        var rows = OutcomeRows(count);
        var where = $"t.{_o("LeaseOwner")} = @owner AND t.{_o("Status")} = {Processing}";
        return _provider switch
        {
            SqlProvider.Oracle => OracleComplete(rows),
            // Seeking by Status would lock other dispatchers' in-flight rows and deadlock, so each row is found by its id.
            SqlProvider.SqlServer => $"""
                UPDATE t SET {CompleteAssignments("t.")}
                FROM {rows} JOIN {_outbox} AS t WITH (FORCESEEK, ROWLOCK) ON t.{_o("Id")} = v.id
                WHERE {where}
                OPTION (LOOP JOIN, FORCE ORDER);
                """,
            SqlProvider.MySql => $"""
                UPDATE {_outbox} AS t JOIN {rows} ON t.{_o("Id")} = v.id
                SET {CompleteAssignments("t.")}
                WHERE {where};
                """,
            _ => $"""
                UPDATE {_outbox} AS t SET {CompleteAssignments(string.Empty)}
                FROM {rows}
                WHERE t.{_o("Id")} = v.id AND {where};
                """,
        };
    }

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

    /// <summary>Returns one column named Value, as EF Core's SqlQueryRaw expects.</summary>
    public string OldestUnsentAvailableAt() =>
        $"SELECT MIN({_o("AvailableAt")}) AS \"Value\" FROM {_outbox} WHERE {_o("Status")} IN ({Pending}, {Processing})";

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

    /// <summary>A derived table named v whose columns are <see cref="OutcomeColumns"/>.</summary>
    private string OutcomeRows(int count)
    {
        var rows = Enumerable.Range(0, count).Select(OutcomeParameters).ToArray();
        var values = string.Join(", ", rows.Select(r => $"({string.Join(", ", r)})"));
        return _provider switch
        {
            // SQLite has no column alias list and names VALUES columns column1..columnN.
            SqlProvider.Sqlite =>
                $"(SELECT {string.Join(", ", OutcomeColumns.Select((c, i) => $"column{i + 1} AS {c}"))} FROM (VALUES {values})) AS v",
            SqlProvider.MySql => $"({UnionRows(rows, string.Empty)}) AS v",
            SqlProvider.Oracle => $"({UnionRows(rows, " FROM dual")}) v",
            _ => $"(VALUES {values}) AS v({string.Join(", ", OutcomeColumns)})",
        };
    }

    private static string[] OutcomeParameters(int index) =>
        [$"@id{index}", $"@status{index}", $"@attempts{index}", $"@availableAt{index}", $"@sentAt{index}", $"@error{index}"];

    private static string UnionRows(string[][] rows, string from)
    {
        var head = $"SELECT {string.Join(", ", rows[0].Select((p, i) => $"{p} AS {OutcomeColumns[i]}"))}{from}";
        return string.Join("\nUNION ALL ", rows.Skip(1).Select(r => $"SELECT {string.Join(", ", r)}{from}").Prepend(head));
    }

    /// <summary>Lease columns are cleared too; <paramref name="target"/> qualifies the assigned columns where the dialect needs it.</summary>
    private string CompleteAssignments(string target, string error = "v.error") => $"""
        {target}{_o("Status")} = v.status,
            {target}{_o("Attempts")} = v.attempts,
            {target}{_o("AvailableAt")} = COALESCE(v.available_at, t.{_o("AvailableAt")}),
            {target}{_o("SentAt")} = v.sent_at,
            {target}{_o("LastError")} = COALESCE({error}, t.{_o("LastError")}),
            {target}{_o("LeaseOwner")} = NULL,
            {target}{_o("LeaseUntil")} = NULL
        """;

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
