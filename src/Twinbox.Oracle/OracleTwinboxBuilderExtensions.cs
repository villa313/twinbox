using Oracle.ManagedDataAccess.Client;
using Twinbox.Oracle;
using Twinbox.Relational;
using Twinbox.Sql;

namespace Twinbox;

public static class OracleTwinboxBuilderExtensions
{
    /// <summary>Stores the outbox and inbox with plain ADO.NET. Save messages in your own transaction with
    /// <c>outbox.CommitAsync(transaction)</c>; handlers write through <see cref="HandlerTransaction"/>.</summary>
    public static TwinboxBuilder UseOracle(this TwinboxBuilder builder, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return builder.UseOracle(o => o.ConnectionString = connectionString);
    }

    public static TwinboxBuilder UseOracle(this TwinboxBuilder builder, Action<OracleStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new OracleStorageOptions();
        configure(options);

        var connectionString = options.ConnectionStringFactory
            ?? (options.ConnectionString is { } fixedConnectionString
                ? _ => fixedConnectionString
                : throw new ArgumentException("Set ConnectionString or ConnectionStringFactory.", nameof(configure)));

        return builder.AddRelationalStore(new RelationalSettings(
            SqlProvider.Oracle,
            options.Schema,
            options.OutboxTable,
            options.InboxTable,
            options.CreateSchemaIfMissing,
            sp => new OracleConnection(connectionString(sp))));
    }
}
