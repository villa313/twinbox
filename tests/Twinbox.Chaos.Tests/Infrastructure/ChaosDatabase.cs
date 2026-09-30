using System.Data.Common;
using Dapper;
using DotNet.Testcontainers.Containers;
using Microsoft.Data.SqlClient;
using Npgsql;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

[assembly: AssemblyFixture(typeof(Twinbox.Chaos.Tests.PostgreSqlDatabase))]
[assembly: AssemblyFixture(typeof(Twinbox.Chaos.Tests.SqlServerDatabase))]

namespace Twinbox.Chaos.Tests;

/// <summary>A database server shared by the tests; each test works in its own schema so nothing leaks between them.</summary>
public abstract class ChaosDatabase : IAsyncLifetime
{
    public abstract string ConnectionString { get; }

    public abstract DbConnection Connect();

    public abstract string Quote(string identifier);

    public abstract string CreateEffectsTable(string schema);

    public abstract ValueTask InitializeAsync();

    public abstract ValueTask DisposeAsync();

    public abstract void UseStore(TwinboxBuilder builder, string schema);

    public static string NewSchema() => $"chaos_{Guid.NewGuid():N}"[..16];

    public string Table(string schema, string name) => $"{Quote(schema)}.{Quote(name)}";

    public async Task<IReadOnlyList<OutboxRow>> OutboxAsync(string schema)
    {
        await using var connection = Connect();
        var rows = await connection.QueryAsync<OutboxRow>($"SELECT * FROM {Table(schema, "TwinboxOutbox")}");
        return [.. rows.OrderBy(r => r.Sequence)];
    }

    /// <summary>When each leased message's lease runs out; read by hand because providers disagree on the CLR type.</summary>
    public async Task<IReadOnlyDictionary<Guid, DateTimeOffset>> LeasesAsync(string schema)
    {
        await using var connection = Connect();
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {Quote("Id")}, {Quote("LeaseUntil")} FROM {Table(schema, "TwinboxOutbox")} WHERE {Quote("LeaseUntil")} IS NOT NULL";
        await using var reader = await command.ExecuteReaderAsync();
        var leases = new Dictionary<Guid, DateTimeOffset>();
        while (await reader.ReadAsync())
        {
            leases[reader.GetGuid(0)] = reader.GetFieldValue<DateTimeOffset>(1);
        }

        return leases;
    }

    public async Task<int> CountAsync(string schema, string table)
    {
        await using var connection = Connect();
        return await connection.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM {Table(schema, table)}");
    }

    /// <summary>Handler side effects, one row per run that committed.</summary>
    public async Task<IReadOnlyList<(string MessageId, string Consumer)>> EffectsAsync(string schema)
    {
        await using var connection = Connect();
        var rows = await connection.QueryAsync<(string, string)>($"SELECT message_id, consumer FROM {Table(schema, "effects")}");
        return [.. rows];
    }

    public async Task PrepareEffectsAsync(string schema)
    {
        await using var connection = Connect();
        await connection.ExecuteAsync(CreateEffectsTable(schema));
    }

    public string InsertEffect(string schema) =>
        $"INSERT INTO {Table(schema, "effects")} (message_id, consumer) VALUES (@messageId, @consumer)";
}

public class PostgreSqlDatabase : ChaosDatabase
{
    private readonly PostgreSqlContainer _container;
    private string? _connectionString;

    public PostgreSqlDatabase()
        : this(fixedPort: false)
    {
    }

    /// <summary>A fixed host port survives a container restart, so clients can reconnect to the same address.</summary>
    protected PostgreSqlDatabase(bool fixedPort)
    {
        var builder = new PostgreSqlBuilder("postgres:17-alpine").WithCommand("-c", "max_connections=400");
        _container = (fixedPort ? builder.WithPortBinding(Ports.Free(), 5432) : builder).Build();
    }

    // Read once: while the container is stopped the port lookup throws, and with a fixed port the address never changes.
    public override string ConnectionString => _connectionString ?? throw new InvalidOperationException("The database hasn't started.");

    protected IContainer Container => _container;

    public override DbConnection Connect() => new NpgsqlConnection(ConnectionString);

    public override string Quote(string identifier) => $"\"{identifier}\"";

    public override string CreateEffectsTable(string schema) =>
        $"CREATE TABLE {Table(schema, "effects")} (message_id varchar(64) NOT NULL, consumer varchar(256) NOT NULL);";

    public override void UseStore(TwinboxBuilder builder, string schema) =>
        builder.UsePostgreSql(o =>
        {
            o.ConnectionString = ConnectionString;
            o.Schema = schema;
        });

    public override async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _connectionString = _container.GetConnectionString();
    }

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

public sealed class SqlServerDatabase : ChaosDatabase
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    public override string ConnectionString => _container.GetConnectionString();

    public override DbConnection Connect() => new SqlConnection(ConnectionString);

    public override string Quote(string identifier) => $"[{identifier}]";

    public override string CreateEffectsTable(string schema) =>
        $"CREATE TABLE {Table(schema, "effects")} (message_id nvarchar(64) NOT NULL, consumer nvarchar(256) NOT NULL);";

    public override void UseStore(TwinboxBuilder builder, string schema) =>
        builder.UseSqlServer(o =>
        {
            o.ConnectionString = ConnectionString;
            o.Schema = schema;
        });

    public override async ValueTask InitializeAsync() => await _container.StartAsync();

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

/// <summary>A PostgreSQL server of its own, which tests may pause, restart or cut connections to.</summary>
public sealed class RestartablePostgreSqlDatabase() : PostgreSqlDatabase(fixedPort: true)
{
    public async Task PauseAsync(TimeSpan duration)
    {
        await Container.PauseAsync();
        try
        {
            await Task.Delay(duration);
        }
        finally
        {
            await Container.UnpauseAsync();
        }
    }

    public async Task RestartAsync()
    {
        await Container.StopAsync();
        await Container.StartAsync();
    }

    /// <summary>Kills every other session, like a failover or an admin terminating connections.</summary>
    public async Task TerminateConnectionsAsync()
    {
        await using var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(ConnectionString) { Pooling = false }.ConnectionString);
        await connection.ExecuteAsync(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()");
    }
}

public sealed class OutboxRow
{
    public long Sequence { get; init; }

    public Guid Id { get; init; }

    public string? PartitionKey { get; init; }

    public int Status { get; init; }

    public int Attempts { get; init; }

    public string? LeaseOwner { get; init; }

    public string? LastError { get; init; }

    public byte[] Payload { get; init; } = [];

    public Numbered Message => DeliveryLog.Read(Payload);
}
