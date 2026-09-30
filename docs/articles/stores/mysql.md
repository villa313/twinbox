# MySQL

`Twinbox.MySql` stores the outbox and inbox with plain ADO.NET using **MySqlConnector**, for apps that use Dapper or
`MySqlCommand`. Your transaction must be a `MySqlConnector.MySqlTransaction`. For EF Core (Pomelo or
`MySql.EntityFrameworkCore`), use the [EF Core store](entity-framework-core.md).

Requires MySQL 8.0 or later (for `SKIP LOCKED`).

## Setup

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseMySql(connectionString)
    .UseKafka("localhost:9092")
    .Route<OrderPlaced>().To("orders"));
```

## Options

| Option | Default | Description |
|---|---|---|
| `ConnectionString` | `null` | Connection string for the database holding the tables. |
| `ConnectionStringFactory` | `null` | Resolved per DI scope, e.g. to pick a tenant's database. Takes precedence over `ConnectionString`. |
| `Schema` | `null` | The database holding the tables; `null` uses the connection's database. Created if missing. |
| `OutboxTable` | `"TwinboxOutbox"` | Outbox table name. |
| `InboxTable` | `"TwinboxInbox"` | Inbox table name. |
| `CreateSchemaIfMissing` | `true` | Create the database (when `Schema` is set) and tables at startup. |

## Sending

```csharp
await using var connection = new MySqlConnection(connectionString);
await connection.OpenAsync(ct);
await using var transaction = await connection.BeginTransactionAsync(ct);

await connection.ExecuteAsync("INSERT INTO orders (id, total) VALUES (@Id, @Total)", order, transaction);
outbox.Send(new OrderPlaced(order.Id));
await outbox.CommitAsync(transaction, ct);
```

## Handlers

```csharp
public class ShipOrder(HandlerTransaction tx) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct) =>
        tx.Connection.ExecuteAsync(
            "UPDATE orders SET status = 'shipping' WHERE id = @OrderId", message, tx.Transaction);
}
```

## Schema

Created at startup when missing (default names):

```sql
CREATE TABLE IF NOT EXISTS `TwinboxOutbox` (
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
    UNIQUE INDEX `IX_TwinboxOutbox_Id` (`Id`),
    INDEX `IX_TwinboxOutbox_Status_AvailableAt` (`Status`, `AvailableAt`),
    INDEX `IX_TwinboxOutbox_PartitionKey_Status` (`PartitionKey`, `Status`));

CREATE TABLE IF NOT EXISTS `TwinboxInbox` (
    `MessageId` varchar(256) NOT NULL,
    `Consumer` varchar(256) NOT NULL,
    `Source` varchar(256) NOT NULL,
    `ProcessedAt` datetime(6) NOT NULL,
    PRIMARY KEY (`MessageId`, `Consumer`),
    INDEX `IX_TwinboxInbox_ProcessedAt` (`ProcessedAt`));
```

Timestamps are stored as UTC `datetime(6)` and ids as `char(36)`, matching what the EF Core MySQL providers create.

## How it works

- **Claiming:** MySQL has no `UPDATE ... RETURNING`, so a claim is three statements in one read-committed transaction:
  lock due keys with `FOR UPDATE SKIP LOCKED`, lease them, and read them back. Read committed avoids gap locks that
  would stall concurrent inserts.
- **Inbox:** `INSERT IGNORE`; a concurrent duplicate waits on the row lock.
- **Retention:** `DELETE ... ORDER BY ... LIMIT @batch`.
