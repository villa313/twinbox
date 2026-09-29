using MySqlConnector;
using Twinbox.MySql;
using Twinbox.Relational;
using Twinbox.Sql;

namespace Twinbox;

public static class MySqlTwinboxBuilderExtensions
{
    /// <summary>Stores the outbox and inbox with plain ADO.NET. Save messages in your own transaction with
    /// <c>outbox.CommitAsync(transaction)</c>; handlers write through <see cref="HandlerTransaction"/>.</summary>
    public static TwinboxBuilder UseMySql(this TwinboxBuilder builder, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return builder.UseMySql(o => o.ConnectionString = connectionString);
    }

    public static TwinboxBuilder UseMySql(this TwinboxBuilder builder, Action<MySqlStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new MySqlStorageOptions();
        configure(options);

        var connectionString = options.ConnectionStringFactory
            ?? (options.ConnectionString is { } fixedConnectionString
                ? _ => fixedConnectionString
                : throw new ArgumentException("Set ConnectionString or ConnectionStringFactory.", nameof(configure)));

        return builder.AddRelationalStore(new RelationalSettings(
            SqlProvider.MySql,
            options.Schema,
            options.OutboxTable,
            options.InboxTable,
            options.CreateSchemaIfMissing,
            sp => new MySqlConnection(connectionString(sp))));
    }
}
