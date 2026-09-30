# SQL Server

`Twinbox.SqlServer` stores the outbox and inbox with plain ADO.NET (`Microsoft.Data.SqlClient`), for apps that use
Dapper or `SqlCommand`. For EF Core, use the [EF Core store](entity-framework-core.md).

## Setup

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseSqlServer(connectionString)
    .UseAzureServiceBus(serviceBusConnectionString)
    .Route<OrderPlaced>().To("orders"));
```

```csharp
twinbox.UseSqlServer(o =>
{
    o.ConnectionString = connectionString;
    o.Schema = "messaging";
    o.CreateSchemaIfMissing = false;   // your migrations create the tables
});
```

## Options

| Option | Default | Description |
|---|---|---|
| `ConnectionString` | `null` | Connection string for the database holding the tables. |
| `ConnectionStringFactory` | `null` | Resolved per DI scope, e.g. to pick a tenant's database. Takes precedence over `ConnectionString`. |
| `Schema` | `null` (`dbo`) | Schema of both tables. |
| `OutboxTable` | `"TwinboxOutbox"` | Outbox table name. |
| `InboxTable` | `"TwinboxInbox"` | Inbox table name. |
| `CreateSchemaIfMissing` | `true` | Create the schema and tables at startup (and in each tenant database on first use). |

One of `ConnectionString` or `ConnectionStringFactory` is required.

## Sending

```csharp
await using var connection = new SqlConnection(connectionString);
await connection.OpenAsync(ct);
await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(ct);

await connection.ExecuteAsync("INSERT INTO dbo.Orders (Id, Total) VALUES (@Id, @Total)", order, transaction);
outbox.Send(new OrderPlaced(order.Id));
await outbox.CommitAsync(transaction, ct);
```

## Handlers

```csharp
public class ShipOrder(HandlerTransaction tx) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct) =>
        tx.Connection.ExecuteAsync(
            "UPDATE dbo.Orders SET Status = 'Shipping' WHERE Id = @OrderId", message, tx.Transaction);
}
```

## Schema

With `CreateSchemaIfMissing`, Twinbox creates the following at startup when it doesn't exist yet (shown with default
names, without the existence checks). Use it as the basis for your own migration when you turn creation off.

```sql
IF SCHEMA_ID(N'dbo') IS NULL EXEC(N'CREATE SCHEMA [dbo]');
CREATE TABLE [TwinboxOutbox] (
    [Sequence] bigint IDENTITY(1,1) NOT NULL CONSTRAINT [PK_TwinboxOutbox] PRIMARY KEY,
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
    [CreatedAt] datetimeoffset(6) NOT NULL,
    [AvailableAt] datetimeoffset(6) NOT NULL,
    [Attempts] int NOT NULL,
    [Status] int NOT NULL,
    [LeaseOwner] nvarchar(256) NULL,
    [LeaseUntil] datetimeoffset(6) NULL,
    [LastError] nvarchar(2000) NULL,
    [SentAt] datetimeoffset(6) NULL);
CREATE UNIQUE INDEX [IX_TwinboxOutbox_Id] ON [TwinboxOutbox] ([Id]);
CREATE INDEX [IX_TwinboxOutbox_Status_AvailableAt] ON [TwinboxOutbox] ([Status], [AvailableAt]);
CREATE INDEX [IX_TwinboxOutbox_PartitionKey_Status] ON [TwinboxOutbox] ([PartitionKey], [Status]);

CREATE TABLE [TwinboxInbox] (
    [MessageId] nvarchar(256) NOT NULL,
    [Consumer] nvarchar(256) NOT NULL,
    [Source] nvarchar(256) NOT NULL,
    [ProcessedAt] datetimeoffset(6) NOT NULL,
    CONSTRAINT [PK_TwinboxInbox] PRIMARY KEY ([MessageId], [Consumer]));
CREATE INDEX [IX_TwinboxInbox_ProcessedAt] ON [TwinboxInbox] ([ProcessedAt]);
```

The layout matches what the EF Core store's `modelBuilder.AddTwinbox()` produces with default names.

## How it works

- **Claiming:** `UPDATE ... OUTPUT inserted.*` over `SELECT TOP (@batch) ... WITH (UPDLOCK, READPAST, ROWLOCK)`, so
  instances skip each other's rows instead of waiting.
- **Inbox:** a conditional insert under `UPDLOCK, HOLDLOCK`; a concurrent duplicate waits for the first to commit or
  roll back, then sees it.
- **Retention:** `DELETE TOP (@batch) ... WITH (READPAST)`.
- Inserts are chunked to stay under SQL Server's 2,100-parameter limit.
