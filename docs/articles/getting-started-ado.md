# Getting started with Dapper and ADO.NET

The ADO.NET stores work with any code that has a `DbConnection` and a `DbTransaction`: Dapper, plain ADO.NET, or
another data library that exposes them. This walkthrough uses PostgreSQL and RabbitMQ.

## 1. Install

```bash
dotnet add package Twinbox
dotnet add package Twinbox.PostgreSql      # or Twinbox.SqlServer / Twinbox.MySql / Twinbox.Oracle
dotnet add package Twinbox.RabbitMQ
```

## 2. Register Twinbox

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UsePostgreSql(connectionString)
    .UseRabbitMQ(o => o.ConnectionUri = new Uri(rabbitUri))
    .Route<OrderPlaced>().To("orders"));
```

The outbox and inbox tables are created on startup. If your migrations own the schema, turn that off and create the
tables yourself (see [Operations](operations.md#schema)):

```csharp
twinbox.UsePostgreSql(o =>
{
    o.ConnectionString = connectionString;
    o.CreateSchemaIfMissing = false;
});
```

## 3. Send

```csharp
await using var connection = new NpgsqlConnection(connectionString);
await connection.OpenAsync();
await using var transaction = await connection.BeginTransactionAsync();

await connection.ExecuteAsync("INSERT INTO orders (id, total) VALUES (@Id, @Total)", order, transaction);
outbox.Send(new OrderPlaced(order.Id));

await outbox.CommitAsync(transaction);   // writes the messages, commits, wakes the dispatcher
```

`CommitAsync` is shorthand for `SaveAsync` followed by `transaction.CommitAsync()`. Use `SaveAsync` when something
else commits the transaction:

```csharp
await outbox.SaveAsync(transaction);     // writes the messages through the transaction
await transaction.CommitAsync();         // the dispatcher picks them up on its next poll
```

The transaction must belong to the database the store is configured for. The messages go into its outbox table.

## 4. Handle

Handlers write through the injected `HandlerTransaction`, so their writes commit together with the inbox entry and
any messages they send:

```csharp
public class ShipOrder(HandlerTransaction tx, IOutbox outbox) : IHandle<OrderPlaced>
{
    public async Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        await tx.Connection.ExecuteAsync(
            "UPDATE orders SET status = 'shipping' WHERE id = @OrderId", message, tx.Transaction);
        outbox.Send(new ShipmentRequested(message.OrderId));   // saved in the same transaction
    }
}
```

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UsePostgreSql(connectionString)
    .UseRabbitMQ(o =>
    {
        o.ConnectionUri = new Uri(rabbitUri);
        o.Listen(queue: "shipping", exchange: "orders");
    })
    .AddHandler<ShipOrder, OrderPlaced>());
```

`HandlerTransaction` is only active while the relational inbox runs a handler. Outside of that, `tx.IsActive` is
false and `tx.Connection` throws.

## Next

- Store pages: [SQL Server](stores/sql-server.md), [PostgreSQL](stores/postgresql.md), [MySQL](stores/mysql.md),
  [Oracle](stores/oracle.md).
- [MongoDB](stores/mongodb.md) works the same way with `outbox.CommitAsync(session)` and `MongoDBHandlerSession`.
- [Handlers](concepts/handlers.md) and [delivery guarantees](concepts/delivery-guarantees.md).
