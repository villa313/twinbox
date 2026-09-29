using System.Data;
using System.Data.Common;

namespace Twinbox.Relational;

internal static partial class DbCommandExtensions
{
    public static DbCommand Command(this DbConnection connection, string sql, DbTransaction? transaction = null)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        OnCommandCreated(command);
        return command;
    }

    public static DbCommand With(this DbCommand command, string name, object? value, DbType type)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        var typed = false;
        OnParameterCreated(parameter, type, ref value, ref typed);
        if (!typed)
        {
            parameter.DbType = type;
        }

        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return command;
    }

    /// <summary>Lets a provider package adapt commands and parameters; compiles away where it isn't implemented.</summary>
    static partial void OnCommandCreated(DbCommand command);

    static partial void OnParameterCreated(DbParameter parameter, DbType type, ref object? value, ref bool typed);
}
