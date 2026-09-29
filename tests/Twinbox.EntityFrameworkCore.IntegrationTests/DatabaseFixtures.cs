using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

public abstract class DatabaseFixture : IAsyncLifetime
{
    private readonly SemaphoreSlim _schemaGate = new(1, 1);
    private bool _schemaCreated;

    public abstract string Name { get; }

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
