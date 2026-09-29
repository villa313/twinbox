using Microsoft.Data.SqlClient;
using Twinbox.SqlServer;
using Twinbox.Relational;
using Twinbox.Sql;

namespace Twinbox;

public static class SqlServerTwinboxBuilderExtensions
{
    /// <summary>
    /// Stores the outbox and inbox with plain ADO.NET. Save messages in your own transaction with
    /// <c>outbox.CommitAsync(transaction)</c>; handlers write through <see cref="HandlerTransaction"/>.
    /// </summary>
    public static TwinboxBuilder UseSqlServer(this TwinboxBuilder builder, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return builder.UseSqlServer(o => o.ConnectionString = connectionString);
    }

    public static TwinboxBuilder UseSqlServer(this TwinboxBuilder builder, Action<SqlServerStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new SqlServerStorageOptions();
        configure(options);

        var connectionString = options.ConnectionStringFactory
            ?? (options.ConnectionString is { } fixedConnectionString
                ? _ => fixedConnectionString
                : throw new ArgumentException("Set ConnectionString or ConnectionStringFactory.", nameof(configure)));

        return builder.AddRelationalStore(new RelationalSettings(
            SqlProvider.SqlServer,
            options.Schema,
            options.OutboxTable,
            options.InboxTable,
            options.CreateSchemaIfMissing,
            sp => new SqlConnection(connectionString(sp))));
    }
}
