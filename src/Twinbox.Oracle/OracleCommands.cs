using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace Twinbox.Relational;

/// <summary>Adapts the shared statements' commands to ODP.NET, which binds by position and names parameters without '@'.</summary>
internal static partial class DbCommandExtensions
{
    static partial void OnCommandCreated(DbCommand command)
    {
        if (command is OracleCommand oracle)
        {
            oracle.BindByName = true;
        }
    }

    static partial void OnParameterCreated(DbParameter parameter, DbType type, ref object? value, ref bool typed)
    {
        if (parameter is not OracleParameter oracle)
        {
            return;
        }

        oracle.ParameterName = oracle.ParameterName.TrimStart('@');
        switch (type)
        {
            // ODP.NET has no DbType.Guid; RAW(16) in .NET byte order matches Oracle's EF Core provider.
            case DbType.Guid:
                oracle.OracleDbType = OracleDbType.Raw;
                value = value is Guid guid ? guid.ToByteArray() : value;
                typed = true;
                break;

            // A RAW bind stops at 32 KB inside PL/SQL, so payloads go in as temporary LOBs.
            case DbType.Binary:
                oracle.OracleDbType = OracleDbType.Blob;
                typed = true;
                break;
        }
    }
}
