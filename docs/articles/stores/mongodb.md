# MongoDB

`Twinbox.MongoDB` stores the outbox and inbox in MongoDB collections and saves messages in your own multi-document
transaction. Transactions need a **replica set** (or a sharded cluster); a standalone server won't do. A single-node
replica set is fine for development.

## Setup

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseMongoDB("mongodb://localhost:27017/?replicaSet=rs0", databaseName: "shop")
    .UseRabbitMq(o => o.ConnectionUri = new Uri(rabbitUri))
    .Route<OrderPlaced>().To("orders"));
```

Share the client your app already uses:

```csharp
builder.Services.AddSingleton<IMongoClient>(new MongoClient(connectionString));

twinbox.UseMongoDB(o =>
{
    o.ClientFactory = sp => sp.GetRequiredService<IMongoClient>();
    o.DatabaseName = "shop";
});
```

## Options

| Option | Default | Description |
|---|---|---|
| `ConnectionString` | `null` | Twinbox creates (and disposes) one client from it. |
| `ClientFactory` | `null` | Resolved per DI scope; takes precedence over `ConnectionString`. Return cached clients, not new ones. |
| `DatabaseName` | `null` | Database holding the collections. |
| `DatabaseNameFactory` | `null` | Resolved per DI scope, e.g. to pick a tenant's database; takes precedence over `DatabaseName`. |
| `OutboxCollection` | `"twinbox_outbox"` | Outbox collection. A `{OutboxCollection}_sequence` collection holds the insertion counter. |
| `InboxCollection` | `"twinbox_inbox"` | Inbox collection. |
| `CreateIndexes` | `true` | Create the indexes at startup (and in each tenant database on first use). |

A client (`ConnectionString` or `ClientFactory`) and a database (`DatabaseName` or `DatabaseNameFactory`) are required.

## Sending

```csharp
using var session = await client.StartSessionAsync(cancellationToken: ct);
session.StartTransaction();

await orders.InsertOneAsync(session, order, cancellationToken: ct);
outbox.Send(new OrderPlaced(order.Id));

await outbox.CommitAsync(session, ct);   // inserts the messages, commits the transaction, wakes the dispatcher
```

`SaveAsync(session)` inserts without committing, for when your code commits itself. The messages go into the
configured database on the session's client, so the session must come from the same cluster.

## Handlers

Inject `MongoHandlerSession` and pass its session to every write, so the writes commit with the inbox entry and any
messages the handler sends:

```csharp
public class ShipOrder(MongoHandlerSession mongo) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct) =>
        mongo.Database.GetCollection<Order>("orders").UpdateOneAsync(
            mongo.Session,
            o => o.Id == message.OrderId,
            Builders<Order>.Update.Set(o => o.Status, "shipping"),
            cancellationToken: ct);
}
```

`mongo.Database` is the Twinbox database reached through the session's client. `MongoHandlerSession` is only active
inside a handler run by the MongoDB inbox; elsewhere `IsActive` is false and its properties throw.

If the transaction fails with a transient error (`TransientTransactionError`, for example a write conflict with a
concurrent duplicate), the inbox retries it, running the handler again, for up to two minutes.

## Indexes

- Outbox: unique on `Sequence`; (`Status`, `AvailableAt`); (`PartitionKey`, `Status`, `Sequence`).
- Inbox: `ProcessedAt`. Entries use a compound `_id` of message id and consumer, so uniqueness doesn't depend on an
  index you manage.

## How it works

- **Claiming:** MongoDB has no `SKIP LOCKED`. The dispatcher scans due documents in sequence order and leases each one
  with a filtered `findOneAndUpdate`; a document another instance leased first simply doesn't match.
- **Ordering:** each document gets a number from an atomic counter when it is inserted. Only the lowest unsent number
  of each partition key is claimable.
- **Retention** deletes in batches by first selecting ids, since `deleteMany` has no limit.

## Limitations

- Batch handlers receive batches of one: the MongoDB inbox doesn't implement batch deduplication.
- The dispatcher needs one `findOneAndUpdate` per claimed message, so claiming costs more round trips than on the
  relational stores.
