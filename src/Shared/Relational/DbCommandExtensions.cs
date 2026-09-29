using System.Data;
using System.Data.Common;

namespace Twinbox.Relational;

internal static class DbCommandExtensions
{
    public static DbCommand Command(this DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command;
    }

    public static DbCommand With(this DbCommand command, string name, object? value, DbType type)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return command;
    }
}
