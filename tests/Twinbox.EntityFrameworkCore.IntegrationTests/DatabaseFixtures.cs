using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Testcontainers.MsSql;
using Testcontainers.Oracle;
using Testcontainers.PostgreSql;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

public abstract class DatabaseFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _schemaGate = new(1, 1);
    private bool _schemaCreated;
    private bool _billingCreated;

    public abstract string Name { get; }

    /// <summary>Both contexts share one database, so the second adds its tables without EnsureCreated.</summary>
    public async Task EnsureBillingSchemaAsync(BillingContext context)
    {
        await _schemaGate.WaitAsync();
        try
        {
            if (!_billingCreated)
            {
                var creator = context.GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalDatabaseCreator>();
                await creator.CreateTablesAsync();
                _billingCreated = true;
            }
        }
        finally
        {
            _schemaGate.Release();
        }
    }

    public async Task EnsureSchemaAsync(ShopContext context)
    {
        await _schemaGate.WaitAsync();
        try
        {
            if (!_schemaCreated)
            {
                await context.Database.EnsureCreatedAsync();
                _schemaCreated = true;
            }
        }
        finally
        {
            _schemaGate.Release();
        }
    }

    public abstract void Configure(DbContextOptionsBuilder options, bool retryOnFailure);

    public abstract ValueTask InitializeAsync();

    public abstract ValueTask DisposeAsync();
}

public sealed class PostgreSqlFixture : DatabaseFixture
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public override string Name => "PostgreSQL";

    /// <summary>Same server, separate database; EnsureCreated creates it on first use.</summary>
    public string ConnectionStringFor(string database) =>
        new Npgsql.NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { Database = database }.ConnectionString;

    public override void Configure(DbContextOptionsBuilder options, bool retryOnFailure) =>
        options.UseNpgsql(_container.GetConnectionString(), o =>
        {
            if (retryOnFailure)
            {
                o.EnableRetryOnFailure();
            }
        });

    public override async ValueTask InitializeAsync() => await _container.StartAsync();

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

public sealed class SqlServerFixture : DatabaseFixture
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    public override string Name => "SQL Server";

    public override void Configure(DbContextOptionsBuilder options, bool retryOnFailure) =>
        options.UseSqlServer(_container.GetConnectionString(), o =>
        {
            if (retryOnFailure)
            {
                o.EnableRetryOnFailure();
            }
        });

    public override async ValueTask InitializeAsync() => await _container.StartAsync();

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

public sealed class OracleFixture : DatabaseFixture
{
    private readonly OracleContainer _container = new OracleBuilder("gvenzl/oracle-free:23-slim-faststart")
        .WithDatabase("FREEPDB1")
        .Build();

    public override string Name => "Oracle";

    public override void Configure(DbContextOptionsBuilder options, bool retryOnFailure) =>
        options.UseOracle(_container.GetConnectionString(), o =>
        {
            if (retryOnFailure)
            {
                o.ExecutionStrategy(d => new Oracle.EntityFrameworkCore.OracleRetryingExecutionStrategy(d));
            }
        });

    public override async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        // Oracle schemas are users, which EnsureCreated doesn't create; the app user needs rights in them.
        var system = new Oracle.ManagedDataAccess.Client.OracleConnectionStringBuilder(_container.GetConnectionString()) { UserID = "system" };
        await using var connection = new Oracle.ManagedDataAccess.Client.OracleConnection(system.ConnectionString);
        await connection.OpenAsync();
        string[] statements =
        [
            """CREATE USER "messaging" NO AUTHENTICATION QUOTA UNLIMITED ON USERS""",
            """CREATE USER "billing" NO AUTHENTICATION QUOTA UNLIMITED ON USERS""",
            "GRANT DBA TO oracle",
        ];
        foreach (var statement in statements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync();
        }
    }

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

public sealed class SqliteFixture : DatabaseFixture
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"twinbox-{Guid.NewGuid():N}.db");

    public override string Name => "SQLite";

    public override void Configure(DbContextOptionsBuilder options, bool retryOnFailure) =>
        options.UseSqlite($"Data Source={_path};Default Timeout=30");

    public override ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public override ValueTask DisposeAsync()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        File.Delete(_path);
        return ValueTask.CompletedTask;
    }
}
