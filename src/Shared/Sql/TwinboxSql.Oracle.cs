using System.Text.RegularExpressions;

namespace Twinbox.Sql;

/// <summary>Oracle binds with ':' and runs one statement per command, so batches become anonymous PL/SQL blocks.</summary>
internal sealed partial class TwinboxSql
{
    /// <summary>Oracle rejects FETCH FIRST with FOR UPDATE, so a cursor locks due rows as it fetches them, skipping held ones.</summary>
    private string OracleClaim(string due) => $"""
        DECLARE
            claimed SYS.ODCINUMBERLIST;
            leased SYS_REFCURSOR;
            CURSOR due IS
                SELECT o.{Sequence} FROM {_outbox} o
                WHERE {ToOracle(due)}
                ORDER BY o.{Sequence}
                FOR UPDATE SKIP LOCKED;
        BEGIN
            OPEN due;
            FETCH due BULK COLLECT INTO claimed LIMIT :batch;
            CLOSE due;
            FORALL i IN 1 .. claimed.COUNT
                UPDATE {_outbox} SET {_o("Status")} = {Processing}, {_o("LeaseOwner")} = :owner, {_o("LeaseUntil")} = :leaseUntil
                WHERE {Sequence} = claimed(i);
            OPEN leased FOR
                SELECT * FROM {_outbox}
                WHERE {Sequence} IN (SELECT COLUMN_VALUE FROM TABLE(claimed))
                ORDER BY {Sequence};
            DBMS_SQL.RETURN_RESULT(leased);
        END;
        """;

    /// <summary>String binds arrive as VARCHAR2, which COALESCE won't mix with the NVARCHAR2 column without TO_NCHAR.</summary>
    private string OracleComplete(int index) => ToOracle($"""
        UPDATE {_outbox} SET
            {_o("Status")} = @status{index},
            {_o("Attempts")} = @attempts{index},
            {_o("AvailableAt")} = COALESCE(@availableAt{index}, {_o("AvailableAt")}),
            {_o("SentAt")} = @sentAt{index},
            {_o("LastError")} = COALESCE(TO_NCHAR(@error{index}), {_o("LastError")}),
            {_o("LeaseOwner")} = NULL,
            {_o("LeaseUntil")} = NULL
        WHERE {_o("Id")} = @id{index} AND {_o("LeaseOwner")} = @owner AND {_o("Status")} = {Processing};
        """);

    private string OraclePurgeOutbox(string expired) => $"""
        DELETE FROM {_outbox} WHERE ROWID IN (
            SELECT ROWID FROM {_outbox} WHERE ({ToOracle(expired)}) AND ROWNUM <= :batch)
        """;

    /// <summary>The hint turns a duplicate key into a skipped row, also once a concurrent duplicate commits.</summary>
    private string OracleInsertInbox(string columns) => $"""
        INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(i ({_i("MessageId")}, {_i("Consumer")})) */ INTO {_inbox} i ({columns})
        VALUES (:messageId, :consumer, :source, :processedAt)
        """;

    private string OraclePurgeInbox() => $"""
        DELETE FROM {_inbox} WHERE ROWID IN (
            SELECT ROWID FROM {_inbox} WHERE {_i("ProcessedAt")} < :before AND ROWNUM <= :batch)
        """;

    private static string OracleBlock(string statements) => $"BEGIN\n{statements}\nEND;";

    private static string ToOracle(string sql) => AtParameter().Replace(sql, ":");

    [GeneratedRegex(@"@(?=\w)", RegexOptions.CultureInvariant)]
    private static partial Regex AtParameter();
}
