using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace Twinbox.Benchmarks.Throughput;

internal abstract class BenchDatabase : IAsyncDisposable
{
    /// <summary>Schema for the ADO.NET store's tables, so they don't collide with the EF Core model's.</summary>
    protected const string AdoSchema = "ado";

    public abstract string Name { get; }

    public virtual bool HasAdoStore => true;

    public virtual string CreateAdoOrders => throw new NotSupportedException();

    public const string InsertAdoOrder = "INSERT INTO ado_orders (reference) VALUES (@reference)";

    public abstract Task StartAsync();

    public virtual string AdoTable(string name) => throw new NotSupportedException();

    public abstract void ConfigureEntityFramework(DbContextOptionsBuilder options);

    public virtual void UseAdoStore(TwinboxBuilder builder) => throw new NotSupportedException();

    public virtual DbConnection Connect() => throw new NotSupportedException();

    public abstract ValueTask DisposeAsync();
}

internal sealed class PostgreSqlBench : BenchDatabase
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public override string Name => "PostgreSQL 17";

    public override string CreateAdoOrders => "CREATE TABLE IF NOT EXISTS ado_orders (reference varchar(64) NOT NULL)";

    public override Task StartAsync() => _container.StartAsync();

    public override string AdoTable(string name) => $"\"{AdoSchema}\".\"{name}\"";

    public override void ConfigureEntityFramework(DbContextOptionsBuilder options) =>
        options.UseNpgsql(_container.GetConnectionString());

    public override void UseAdoStore(TwinboxBuilder builder) => builder.UsePostgreSql(o =>
    {
        o.ConnectionString = _container.GetConnectionString();
        o.Schema = AdoSchema;
    });

    public override DbConnection Connect() => new NpgsqlConnection(_container.GetConnectionString());

    public override ValueTask DisposeAsync() => _container.DisposeAsync();
}

internal sealed class SqlServerBench : BenchDatabase
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    public override string Name => "SQL Server 2022";

    public override string CreateAdoOrders =>
        "IF OBJECT_ID(N'ado_orders', N'U') IS NULL CREATE TABLE ado_orders (reference nvarchar(64) NOT NULL)";

    public override Task StartAsync() => _container.StartAsync();

    public override string AdoTable(string name) => $"[{AdoSchema}].[{name}]";

    public override void ConfigureEntityFramework(DbContextOptionsBuilder options) =>
        options.UseSqlServer(_container.GetConnectionString());

    public override void UseAdoStore(TwinboxBuilder builder) => builder.UseSqlServer(o =>
    {
        o.ConnectionString = _container.GetConnectionString();
        o.Schema = AdoSchema;
    });

    public override DbConnection Connect() => new SqlConnection(_container.GetConnectionString());

    public override ValueTask DisposeAsync() => _container.DisposeAsync();
}

internal sealed class SqliteBench : BenchDatabase
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"twinbox-bench-{Guid.NewGuid():N}.db");

    public override string Name => "SQLite (file)";

    public override bool HasAdoStore => false;

    public override Task StartAsync() => Task.CompletedTask;

    public override void ConfigureEntityFramework(DbContextOptionsBuilder options) =>
        options.UseSqlite($"Data Source={_path};Default Timeout=30");

    public override ValueTask DisposeAsync()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
        {
            File.Delete(file);
        }

        return ValueTask.CompletedTask;
    }
}
