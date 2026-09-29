using System.Data.Common;
using Twinbox.Sql;

namespace Twinbox.Relational;

internal sealed record RelationalSettings(
    SqlProvider Provider,
    string? Schema,
    string OutboxTable,
    string InboxTable,
    bool CreateSchemaIfMissing,
    Func<IServiceProvider, DbConnection> CreateConnection);
