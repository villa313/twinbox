using Npgsql;
using Twinbox.PostgreSql;
using Twinbox.Relational;
using Twinbox.Sql;

namespace Twinbox;

public static class PostgreSqlTwinboxBuilderExtensions
{
    /// <summary>
    /// Stores the outbox and inbox with plain ADO.NET. Save messages in your own transaction with
    /// <c>outbox.CommitAsync(transaction)</c>; handlers write through <see cref="HandlerTransaction"/>.
    /// </summary>
    public static TwinboxBuilder UsePostgreSql(this TwinboxBuilder builder, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return builder.UsePostgreSql(o => o.ConnectionString = connectionString);
    }

    public static TwinboxBuilder UsePostgreSql(this TwinboxBuilder builder, Action<PostgreSqlStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new PostgreSqlStorageOptions();
        configure(options);

        var connectionString = options.ConnectionStringFactory
            ?? (options.ConnectionString is { } fixedConnectionString
                ? _ => fixedConnectionString
                : throw new ArgumentException("Set ConnectionString or ConnectionStringFactory.", nameof(configure)));

        return builder.AddRelationalStore(new RelationalSettings(
            SqlProvider.PostgreSql,
            options.Schema,
            options.OutboxTable,
            options.InboxTable,
            options.CreateSchemaIfMissing,
            sp => new NpgsqlConnection(connectionString(sp))));
    }
}
