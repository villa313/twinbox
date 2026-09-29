using System.Data;
using System.Data.Common;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore.Sql;

internal static class Parameters
{
    public static DbParameter Create(DbContext context, string name, object? value, DbType type)
    {
        using var command = context.Database.GetDbConnection().CreateCommand();
        if ((type is DbType.DateTimeOffset or DbType.Guid) && EntityFrameworkSql.IsMySql(context.Database.ProviderName))
        {
            return MySql(context, command, name, value, type);
        }

        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        // EF Core's SQLite provider stores GUIDs as upper-case text, so raw SQL has to compare against the same form.
        if (value is Guid guid && context.Database.ProviderName == EntityFrameworkSql.SqliteProvider)
        {
            value = guid.ToString().ToUpperInvariant();
            type = DbType.String;
        }

        // ODP.NET names parameters without the '@', and Oracle's EF provider stores GUIDs as RAW(16) in .NET byte order.
        if (context.Database.ProviderName == EntityFrameworkSql.OracleProvider)
        {
            parameter.ParameterName = name.TrimStart('@');
            if (value is Guid oracleGuid)
            {
                value = oracleGuid.ToByteArray();
                type = DbType.Binary;
            }
        }

        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }

    /// <summary>Binds through the provider's own mapping, so timestamps go to UTC and ids to text the way EF writes them.</summary>
    private static DbParameter MySql(DbContext context, DbCommand command, string name, object? value, DbType type)
    {
        if (value is DateTimeOffset timestamp)
        {
            // MySQL rounds stored timestamps to the column's precision (whole seconds for Oracle's provider by default);
            // rounding bound values the same way keeps a message saved just now due for an immediate claim.
            value = Round(timestamp, FractionalDigits(context));
        }

        var clrType = type == DbType.Guid ? typeof(Guid) : typeof(DateTimeOffset);
        var mapping = context.GetService<IRelationalTypeMappingSource>().FindMapping(clrType)
            ?? throw new NotSupportedException($"{context.Database.ProviderName} has no mapping for {clrType.Name}.");
        return mapping.CreateParameter(command, name, value, nullable: true);
    }

    private static int FractionalDigits(DbContext context)
    {
        var property = context.Model.FindEntityType(TwinboxEntities.Outbox)?.FindProperty(nameof(OutboxMessage.AvailableAt));
        if (property?.GetPrecision() is { } precision)
        {
            return precision;
        }

        var columnType = property?.GetColumnType() ?? string.Empty;
        var open = columnType.IndexOf('(', StringComparison.Ordinal);
        return open >= 0 && int.TryParse(columnType.AsSpan(open + 1).TrimEnd(')'), NumberStyles.None, CultureInfo.InvariantCulture, out var digits)
            ? digits
            : 0;
    }

    private static DateTimeOffset Round(DateTimeOffset timestamp, int fractionalDigits)
    {
        var unit = TimeSpan.TicksPerSecond / (long)Math.Pow(10, Math.Clamp(fractionalDigits, 0, 7));
        return new DateTimeOffset((timestamp.UtcTicks + (unit / 2)) / unit * unit, TimeSpan.Zero);
    }
}
