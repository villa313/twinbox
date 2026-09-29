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
        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }
}
