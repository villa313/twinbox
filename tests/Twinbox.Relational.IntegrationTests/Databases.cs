using System.Data.Common;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Testcontainers.MsSql;
using Testcontainers.MySql;
using Testcontainers.Oracle;
using Testcontainers.PostgreSql;

namespace Twinbox.Relational.IntegrationTests;

public abstract class Database : IAsyncLifetime
{
    public abstract string ConnectionString { get; }

    /// <summary>Table the tests' own handlers and requests write to, next to the Twinbox tables.</summary>
    public abstract string CreateOrdersTable { get; }

    public virtual string InsertOrder => "INSERT INTO orders (reference) VALUES (@reference)";

    /// <summary>Another system's outbox table, keyed the way that system would key it in this database.</summary>
    public abstract LegacyOutbox Legacy { get; }

    public abstract void UseStore(TwinboxBuilder builder);

    public abstract DbConnection Connect();

    public abstract ValueTask InitializeAsync();

    public abstract ValueTask DisposeAsync();
}

public sealed record LegacyOutbox(string Create, string Insert, string SelectPending, string MarkImported, object NewId);

public sealed class PostgreSqlDatabase : Database
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public override string ConnectionString => _container.GetConnectionString();

    public override string CreateOrdersTable =>
        """CREATE TABLE IF NOT EXISTS orders (reference varchar(64) PRIMARY KEY);""";

    public override LegacyOutbox Legacy => new(
        """
        CREATE TABLE IF NOT EXISTS legacy_outbox (
            id uuid PRIMARY KEY, name text NOT NULL, content text NOT NULL, headers text, partition_key text,
            imported boolean NOT NULL DEFAULT false);
        """,
        "INSERT INTO legacy_outbox (id, name, content, headers, partition_key) VALUES (@id, @name, @content, @headers, @partitionKey)",
        """
        SELECT id AS "Id", name AS "Name", content AS "Content", headers AS "Headers", partition_key AS "PartitionKey"
        FROM legacy_outbox WHERE NOT imported ORDER BY id LIMIT @batch
        """,
        "UPDATE legacy_outbox SET imported = true WHERE id = @id",
        Guid.NewGuid());

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

public sealed class MySqlDatabase : Database
{
    // Root, because the "messaging" schema is a separate MySQL database the store creates.
    private readonly MySqlContainer _container = new MySqlBuilder("mysql:8.4").WithUsername("root").Build();

    public override string ConnectionString => _container.GetConnectionString();

    public override string CreateOrdersTable =>
        "CREATE TABLE IF NOT EXISTS orders (reference varchar(64) PRIMARY KEY);";

    public override LegacyOutbox Legacy => new(
        """
        CREATE TABLE IF NOT EXISTS legacy_outbox (
            id bigint PRIMARY KEY, name varchar(200) NOT NULL, content text NOT NULL, headers text, partition_key varchar(200),
            imported bool NOT NULL DEFAULT false);
        """,
        "INSERT INTO legacy_outbox (id, name, content, headers, partition_key) VALUES (@id, @name, @content, @headers, @partitionKey)",
        """
        SELECT id AS Id, name AS Name, content AS Content, headers AS Headers, partition_key AS PartitionKey
        FROM legacy_outbox WHERE NOT imported ORDER BY id LIMIT @batch
        """,
        "UPDATE legacy_outbox SET imported = true WHERE id = @id",
        Random.Shared.NextInt64(1, long.MaxValue));

    public override void UseStore(TwinboxBuilder builder) =>
        builder.UseMySql(o =>
        {
            o.ConnectionString = ConnectionString;
            o.Schema = "messaging";
        });

    public override DbConnection Connect() => new MySqlConnection(ConnectionString);

    public override async ValueTask InitializeAsync() => await _container.StartAsync();

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

public sealed class SqlServerDatabase : Database
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    public override string ConnectionString => _container.GetConnectionString();

    public override string CreateOrdersTable =>
        "IF OBJECT_ID(N'orders', N'U') IS NULL CREATE TABLE orders (reference nvarchar(64) NOT NULL PRIMARY KEY);";

    public override LegacyOutbox Legacy => new(
        """
        IF OBJECT_ID(N'legacy_outbox', N'U') IS NULL CREATE TABLE legacy_outbox (
            id uniqueidentifier NOT NULL PRIMARY KEY, name nvarchar(200) NOT NULL, content nvarchar(max) NOT NULL,
            headers nvarchar(max) NULL, partition_key nvarchar(200) NULL, imported bit NOT NULL DEFAULT 0);
        """,
        "INSERT INTO legacy_outbox (id, name, content, headers, partition_key) VALUES (@id, @name, @content, @headers, @partitionKey)",
        """
        SELECT TOP (@batch) id AS Id, name AS Name, content AS Content, headers AS Headers, partition_key AS PartitionKey
        FROM legacy_outbox WHERE imported = 0 ORDER BY id
        """,
        "UPDATE legacy_outbox SET imported = 1 WHERE id = @id",
        Guid.NewGuid());

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

public sealed class OracleDatabase : Database
{
    private readonly OracleContainer _container = new OracleBuilder("gvenzl/oracle-free:23-slim-faststart")
        .WithDatabase("FREEPDB1")
        .Build();

    public override string ConnectionString => _container.GetConnectionString();

    public override string CreateOrdersTable => """
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE orders (reference NVARCHAR2(64) PRIMARY KEY)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
        """;

    public override string InsertOrder => "INSERT INTO orders (reference) VALUES (:reference)";

    public override LegacyOutbox Legacy => new(
        """
        BEGIN
            EXECUTE IMMEDIATE 'CREATE TABLE legacy_outbox (
                id NUMBER(19) PRIMARY KEY, name NVARCHAR2(200) NOT NULL, content NVARCHAR2(2000) NOT NULL,
                headers NVARCHAR2(2000), partition_key NVARCHAR2(200), imported NUMBER(1) DEFAULT 0 NOT NULL)';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
        """,
        "INSERT INTO legacy_outbox (id, name, content, headers, partition_key) VALUES (:id, :name, :content, :headers, :partitionKey)",
        """
        SELECT id AS "Id", name AS "Name", content AS "Content", headers AS "Headers", partition_key AS "PartitionKey"
        FROM legacy_outbox WHERE imported = 0 ORDER BY id FETCH FIRST :batch ROWS ONLY
        """,
        "UPDATE legacy_outbox SET imported = 1 WHERE id = :id",
        Random.Shared.NextInt64(1, 1_000_000_000));

    // Unquoted, so the tests' messaging."TwinboxOutbox" resolves to it.
    public override void UseStore(TwinboxBuilder builder) =>
        builder.UseOracle(o =>
        {
            o.ConnectionString = ConnectionString;
            o.Schema = "MESSAGING";
        });

    public override DbConnection Connect() => new OracleConnection(ConnectionString);

    public override async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        // Oracle schemas are users; the app user needs rights to create and write tables in it.
        var system = new OracleConnectionStringBuilder(ConnectionString) { UserID = "system" };
        await using var connection = new OracleConnection(system.ConnectionString);
        await connection.OpenAsync();
        string[] statements = ["CREATE USER MESSAGING NO AUTHENTICATION QUOTA UNLIMITED ON USERS", "GRANT DBA TO oracle"];
        foreach (var statement in statements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync();
        }
    }

    public override async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
