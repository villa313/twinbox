using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace Twinbox.EntityFrameworkCore.Sql;

internal static class Parameters
{
    public static DbParameter Create(DbContext context, string name, object? value, DbType type)
    {
        using var command = context.Database.GetDbConnection().CreateCommand();
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        // EF Core's SQLite provider stores GUIDs as upper-case text, so raw SQL has to compare against the same form.
        if (value is Guid guid && context.Database.ProviderName == EntityFrameworkSql.SqliteProvider)
        {
            value = guid.ToString().ToUpperInvariant();
            type = DbType.String;
        }

        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }
}
