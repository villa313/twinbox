using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Testcontainers.MsSql;
using Testcontainers.MySql;
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

public sealed class MySqlFixture : DatabaseFixture
{
    // Root, because the model's schemas are separate MySQL databases that EnsureCreated has to create.
    private readonly MySqlContainer _container = new MySqlBuilder("mysql:8.4").WithUsername("root").Build();

    public override string Name => "MySQL";

    public override void Configure(DbContextOptionsBuilder options, bool retryOnFailure) =>
        options.UseMySQL(_container.GetConnectionString(), o =>
        {
            if (retryOnFailure)
            {
                o.EnableRetryOnFailure();
            }
        });

    public override async ValueTask InitializeAsync() => await _container.StartAsync();

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
