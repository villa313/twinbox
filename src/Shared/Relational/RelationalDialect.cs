using System.Text;
using Twinbox.Sql;

namespace Twinbox.Relational;

/// <summary>Fixed table layout for the ADO.NET stores; matches what EF Core's AddTwinbox() creates with default names.</summary>
internal sealed class RelationalDialect
{
    public static readonly string[] OutboxColumns =
    [
        "Id", "MessageName", "Transport", "Destination", "PartitionKey", "TenantId", "Payload", "ContentType", "Headers",
        "TraceParent", "CreatedAt", "AvailableAt", "Attempts", "Status", "LeaseOwner", "LeaseUntil", "LastError", "SentAt",
    ];

    private readonly RelationalSettings _settings;

    public RelationalDialect(RelationalSettings settings)
    {
        _settings = settings;
        Outbox = Table(settings.OutboxTable);
        Inbox = Table(settings.InboxTable);
        Sql = new TwinboxSql(settings.Provider, Outbox, Inbox, Quote, Quote);
    }

    public string Outbox { get; }

    public string Inbox { get; }

    public TwinboxSql Sql { get; }

    public string Quote(string identifier) => _settings.Provider switch
    {
        SqlProvider.SqlServer => $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]",
        SqlProvider.MySql => $"`{identifier.Replace("`", "``", StringComparison.Ordinal)}`",
        _ => $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"",
    };

    public string Insert(int rows)
    {
        var sql = new StringBuilder($"INSERT INTO {Outbox} ({string.Join(", ", OutboxColumns.Select(Quote))}) VALUES ");
        for (var row = 0; row < rows; row++)
        {
            sql.Append(row == 0 ? "(" : ", (")
                .Append(string.Join(", ", OutboxColumns.Select(c => $"@{c}{row}")))
                .Append(')');
        }

        return sql.Append(';').ToString();
    }

    public string CreateSchema() => _settings.Provider switch
    {
        SqlProvider.SqlServer => SqlServerSchema(),
        SqlProvider.MySql => MySqlSchema(),
        _ => PostgreSqlSchema(),
    };

    private string Table(string name) =>
        _settings.Schema is null ? Quote(name) : $"{Quote(_settings.Schema)}.{Quote(name)}";

    private string SqlServerSchema()
    {
        var schema = _settings.Schema ?? "dbo";
        return $"""
            IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA {Quote(schema)}');
            IF OBJECT_ID(N'{Outbox}', N'U') IS NULL
            BEGIN
                CREATE TABLE {Outbox} (
                    [Sequence] bigint IDENTITY(1,1) NOT NULL CONSTRAINT [PK_{_settings.OutboxTable}] PRIMARY KEY,
                    [Id] uniqueidentifier NOT NULL,
                    [MessageName] nvarchar(256) NOT NULL,
                    [Transport] nvarchar(64) NOT NULL,
                    [Destination] nvarchar(256) NOT NULL,
                    [PartitionKey] nvarchar(256) NULL,
                    [TenantId] nvarchar(128) NULL,
                    [Payload] varbinary(max) NOT NULL,
                    [ContentType] nvarchar(128) NOT NULL,
                    [Headers] nvarchar(max) NOT NULL,
                    [TraceParent] nvarchar(64) NULL,
                    [CreatedAt] datetimeoffset NOT NULL,
                    [AvailableAt] datetimeoffset NOT NULL,
                    [Attempts] int NOT NULL,
                    [Status] int NOT NULL,
                    [LeaseOwner] nvarchar(256) NULL,
                    [LeaseUntil] datetimeoffset NULL,
                    [LastError] nvarchar(2000) NULL,
                    [SentAt] datetimeoffset NULL);
                CREATE UNIQUE INDEX [IX_{_settings.OutboxTable}_Id] ON {Outbox} ([Id]);
                CREATE INDEX [IX_{_settings.OutboxTable}_Status_AvailableAt] ON {Outbox} ([Status], [AvailableAt]);
                CREATE INDEX [IX_{_settings.OutboxTable}_PartitionKey_Status] ON {Outbox} ([PartitionKey], [Status]);
            END;
            IF OBJECT_ID(N'{Inbox}', N'U') IS NULL
            BEGIN
                CREATE TABLE {Inbox} (
                    [MessageId] nvarchar(256) NOT NULL,
                    [Consumer] nvarchar(256) NOT NULL,
                    [Source] nvarchar(256) NOT NULL,
                    [ProcessedAt] datetimeoffset NOT NULL,
                    CONSTRAINT [PK_{_settings.InboxTable}] PRIMARY KEY ([MessageId], [Consumer]));
                CREATE INDEX [IX_{_settings.InboxTable}_ProcessedAt] ON {Inbox} ([ProcessedAt]);
            END;
            """;
    }

    // MySQL has no CREATE INDEX IF NOT EXISTS, so the indexes are declared inline. Timestamps are stored as UTC
    // datetime(6) and ids as char(36), matching what the EF Core MySQL providers create.
    private string MySqlSchema()
    {
        var create = _settings.Schema is null ? string.Empty : $"CREATE DATABASE IF NOT EXISTS {Quote(_settings.Schema)};";
        return $"""
            {create}
            CREATE TABLE IF NOT EXISTS {Outbox} (
                `Sequence` bigint NOT NULL AUTO_INCREMENT,
                `Id` char(36) CHARACTER SET ascii NOT NULL,
                `MessageName` varchar(256) NOT NULL,
                `Transport` varchar(64) NOT NULL,
                `Destination` varchar(256) NOT NULL,
                `PartitionKey` varchar(256) NULL,
                `TenantId` varchar(128) NULL,
                `Payload` longblob NOT NULL,
                `ContentType` varchar(128) NOT NULL,
                `Headers` longtext NOT NULL,
                `TraceParent` varchar(64) NULL,
                `CreatedAt` datetime(6) NOT NULL,
                `AvailableAt` datetime(6) NOT NULL,
                `Attempts` int NOT NULL,
                `Status` int NOT NULL,
                `LeaseOwner` varchar(256) NULL,
                `LeaseUntil` datetime(6) NULL,
                `LastError` varchar(2000) NULL,
                `SentAt` datetime(6) NULL,
                PRIMARY KEY (`Sequence`),
                UNIQUE INDEX {Quote($"IX_{_settings.OutboxTable}_Id")} (`Id`),
                INDEX {Quote($"IX_{_settings.OutboxTable}_Status_AvailableAt")} (`Status`, `AvailableAt`),
                INDEX {Quote($"IX_{_settings.OutboxTable}_PartitionKey_Status")} (`PartitionKey`, `Status`));
            CREATE TABLE IF NOT EXISTS {Inbox} (
                `MessageId` varchar(256) NOT NULL,
                `Consumer` varchar(256) NOT NULL,
                `Source` varchar(256) NOT NULL,
                `ProcessedAt` datetime(6) NOT NULL,
                PRIMARY KEY (`MessageId`, `Consumer`),
                INDEX {Quote($"IX_{_settings.InboxTable}_ProcessedAt")} (`ProcessedAt`));
            """;
    }

    private string PostgreSqlSchema()
    {
        var create = _settings.Schema is null ? string.Empty : $"CREATE SCHEMA IF NOT EXISTS {Quote(_settings.Schema)};";
        return $"""
            {create}
            CREATE TABLE IF NOT EXISTS {Outbox} (
                "Sequence" bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                "Id" uuid NOT NULL,
                "MessageName" varchar(256) NOT NULL,
                "Transport" varchar(64) NOT NULL,
                "Destination" varchar(256) NOT NULL,
                "PartitionKey" varchar(256) NULL,
                "TenantId" varchar(128) NULL,
                "Payload" bytea NOT NULL,
                "ContentType" varchar(128) NOT NULL,
                "Headers" text NOT NULL,
                "TraceParent" varchar(64) NULL,
                "CreatedAt" timestamptz NOT NULL,
                "AvailableAt" timestamptz NOT NULL,
                "Attempts" integer NOT NULL,
                "Status" integer NOT NULL,
                "LeaseOwner" varchar(256) NULL,
                "LeaseUntil" timestamptz NULL,
                "LastError" varchar(2000) NULL,
                "SentAt" timestamptz NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS "IX_{_settings.OutboxTable}_Id" ON {Outbox} ("Id");
            CREATE INDEX IF NOT EXISTS "IX_{_settings.OutboxTable}_Status_AvailableAt" ON {Outbox} ("Status", "AvailableAt");
            CREATE INDEX IF NOT EXISTS "IX_{_settings.OutboxTable}_PartitionKey_Status" ON {Outbox} ("PartitionKey", "Status");
            CREATE TABLE IF NOT EXISTS {Inbox} (
                "MessageId" varchar(256) NOT NULL,
                "Consumer" varchar(256) NOT NULL,
                "Source" varchar(256) NOT NULL,
                "ProcessedAt" timestamptz NOT NULL,
                PRIMARY KEY ("MessageId", "Consumer"));
            CREATE INDEX IF NOT EXISTS "IX_{_settings.InboxTable}_ProcessedAt" ON {Inbox} ("ProcessedAt");
            """;
    }
}
