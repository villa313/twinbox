using System.Globalization;
using System.Text;
using Twinbox.Sql;
using Twinbox.Storage;

namespace Twinbox.Relational;

/// <summary>Statements behind <see cref="IOutboxAdmin"/>. None ends in ';', which Oracle rejects outside PL/SQL.</summary>
internal sealed partial class RelationalDialect
{
    public string AdminQuery(OutboxQuery query, bool hasCursor, bool searchesId)
    {
        var sql = new StringBuilder("SELECT ");
        if (_settings.Provider == SqlProvider.SqlServer)
        {
            sql.Append("TOP (@take) ");
        }

        sql.Append(CultureInfo.InvariantCulture, $"* FROM {Outbox} WHERE 1 = 1");
        if (hasCursor)
        {
            sql.Append(CultureInfo.InvariantCulture, $" AND {Quote("Sequence")} < {Parameter("before")}");
        }

        if (query.Status is not null)
        {
            sql.Append(CultureInfo.InvariantCulture, $" AND {Quote("Status")} = {Parameter("status")}");
        }

        if (query.Destination is not null)
        {
            sql.Append(CultureInfo.InvariantCulture, $" AND {Quote("Destination")} = {Parameter("destination")}");
        }

        if (query.MessageName is not null)
        {
            sql.Append(CultureInfo.InvariantCulture, $" AND {Quote("MessageName")} = {Parameter("name")}");
        }

        if (query.Search is not null)
        {
            var byKey = $"{Quote("PartitionKey")} = {Parameter("search")}";
            sql.Append(searchesId ? $" AND ({Quote("Id")} = {Parameter("searchId")} OR {byKey})" : $" AND {byKey}");
        }

        sql.Append(CultureInfo.InvariantCulture, $" ORDER BY {Quote("Sequence")} DESC");
        sql.Append(_settings.Provider switch
        {
            SqlProvider.SqlServer => string.Empty,
            SqlProvider.Oracle => " FETCH FIRST :take ROWS ONLY",
            _ => " LIMIT @take",
        });
        return sql.ToString();
    }

    public string AdminGet() => $"SELECT * FROM {Outbox} WHERE {Quote("Id")} = {Parameter("id")}";

    public string AdminReplay(int ids) => $"""
        UPDATE {Outbox} SET
            {Quote("Status")} = {(int)OutboxMessageStatus.Pending},
            {Quote("Attempts")} = 0,
            {Quote("AvailableAt")} = {Parameter("now")},
            {Quote("LastError")} = NULL,
            {Quote("SentAt")} = NULL,
            {Quote("LeaseOwner")} = NULL,
            {Quote("LeaseUntil")} = NULL
        WHERE {Quote("Status")} IN ({(int)OutboxMessageStatus.Sent}, {(int)OutboxMessageStatus.Dead}) AND {IdList(ids)}
        """;

    public string AdminDelete(int ids) => $"DELETE FROM {Outbox} WHERE {IdList(ids)}";

    /// <summary>Oracle binds with ':'; the other providers accept '@'. Commands still add parameters as "@name".</summary>
    private string Parameter(string name) => (_settings.Provider == SqlProvider.Oracle ? ":" : "@") + name;

    private string IdList(int count) =>
        $"{Quote("Id")} IN ({string.Join(", ", Enumerable.Range(0, count).Select(i => Parameter($"id{i}")))})";
}
