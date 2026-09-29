using System.Data.Common;
using Microsoft.Data.SqlClient;
using Npgsql;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Twinbox.Relational.IntegrationTests;

public abstract class Database : IAsyncLifetime
{
    public abstract string ConnectionString { get; }

    /// <summary>Table the tests' own handlers and requests write to, next to the Twinbox tables.</summary>
    public abstract string CreateOrdersTable { get; }

    public abstract void UseStore(TwinboxBuilder builder);

    public abstract DbConnection Connect();

    public abstract ValueTask InitializeAsync();

    public abstract ValueTask DisposeAsync();
}

public sealed class PostgreSqlDatabase : Database
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public override string ConnectionString => _container.GetConnectionString();

    public override string CreateOrdersTable =>
        """CREATE TABLE IF NOT EXISTS orders (reference varchar(64) PRIMARY KEY);""";

    public override void UseStore(TwinboxBuilder builder) =>
        builder.UsePostgreSql(o =>
        {
            o.ConnectionString = ConnectionString;
            o.Schema = "messaging";
        });

    public override DbConnection Connect() => new NpgsqlConnection(ConnectionString);

    public override async ValueTask InitializeAsync() => await _container.StartAsync();

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

public sealed class SqlServerDatabase : Database
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    public override string ConnectionString => _container.GetConnectionString();

    public override string CreateOrdersTable =>
        "IF OBJECT_ID(N'orders', N'U') IS NULL CREATE TABLE orders (reference nvarchar(64) NOT NULL PRIMARY KEY);";

    public override void UseStore(TwinboxBuilder builder) =>
        builder.UseSqlServer(o =>
        {
            o.ConnectionString = ConnectionString;
            o.Schema = "messaging";
        });

    public override DbConnection Connect() => new SqlConnection(ConnectionString);

    public override async ValueTask InitializeAsync() => await _container.StartAsync();

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
