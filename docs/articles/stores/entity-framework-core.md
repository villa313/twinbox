# EF Core

`Twinbox.EntityFrameworkCore` stores the outbox and inbox in your own `DbContext`. Messages are saved by the same
`SaveChanges` (or transaction) as your entities.

Supported providers: SQL Server, PostgreSQL (Npgsql), MySQL (Pomelo or `MySql.EntityFrameworkCore`), Oracle
(`Oracle.EntityFrameworkCore`) and SQLite. Other providers throw `NotSupportedException` on first use.

## Setup

```csharp
builder.Services.AddDbContext<ShopContext>(o => o.UseNpgsql(connectionString));
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<ShopContext>()
    .UseKafka("localhost:9092")
    .Route<OrderPlaced>().To("orders"));
```

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.AddTwinbox();                 // or AddTwinbox(o => { o.Schema = "messaging"; })
}
```

`UseEntityFrameworkCore<TContext>()` must come after `AddDbContext<TContext>()` (or `AddDbContextPool`); it wraps
that registration so every resolved context is tied to the scope's outbox, and adds a save interceptor. A context
without `AddTwinbox()` in its model throws on first use, telling you to add it and a migration.

### `TwinboxModelOptions`

| Option | Default | Description |
|---|---|---|
| `Schema` | `null` | Schema for both tables; `null` uses the model's default. |
| `OutboxTable` | `null` | Table name; `null` keeps `TwinboxOutbox`, which naming conventions (e.g. snake_case) still rewrite. |
| `InboxTable` | `null` | Table name; `null` keeps `TwinboxInbox`, likewise. |

## How saving works

- `SaveChanges` / `SaveChangesAsync` adds the scope's buffered messages to the change tracker before saving.
- Without an explicit transaction, the dispatcher is woken right after the save. With one, it's woken when the
  transaction commits.
- Pooled contexts from `AddDbContextPool` are enlisted in the scope that resolves them.
- A context you construct yourself, or create through `IDbContextFactory<T>`, needs `context.EnlistOutbox(outbox)`.

## Table layout

The outbox table has an auto-increment `Sequence` primary key (insertion order, append-only clustered index), a unique
index on `Id`, and indexes on (`Status`, `AvailableAt`) and (`PartitionKey`, `Status`). The inbox table's key is
(`MessageId`, `Consumer`), with an index on `ProcessedAt`. Timestamps use microsecond precision. On Oracle, the payload
and headers use `BLOB` and `NCLOB` columns.

With default names, the layout matches what the ADO.NET stores create, so a service can switch between EF Core and
Dapper without a data migration.

To look at or clean up rows directly:

```csharp
using Twinbox.EntityFrameworkCore;   // TwinboxOutbox()
using Twinbox.Storage;               // OutboxMessageStatus

var dead = await db.TwinboxOutbox()
    .Where(m => m.Status == OutboxMessageStatus.Dead)
    .OrderByDescending(m => m.CreatedAt)
    .Take(20)
    .ToListAsync();
```

## Claiming by provider

| Provider | Claim |
|---|---|
| SQL Server | `UPDATE ... OUTPUT` over `TOP (n) WITH (UPDLOCK, READPAST, ROWLOCK)` |
| PostgreSQL | One `UPDATE ... RETURNING` over a `FOR UPDATE SKIP LOCKED` selection |
| Oracle | A PL/SQL block that locks due rows with `FOR UPDATE SKIP LOCKED` and leases them |
| MySQL | `SELECT ... FOR UPDATE SKIP LOCKED`, update and read back in one transaction (MySQL 8.0+) |
| SQLite | `UPDATE ... RETURNING` (SQLite serializes writers) |

## Handlers

The inbox resolves your context from the handler's scope, begins a transaction inside the provider's execution
strategy, inserts the inbox entry, runs the handler, then calls `SaveChanges` and commits. Handlers just change
entities:

```csharp
public class ShipOrder(ShopContext db) : IHandle<OrderPlaced>
{
    public async Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        var order = await db.Orders.SingleAsync(o => o.Id == message.OrderId, ct);
        order.Ship();
    }
}
```

A retrying execution strategy reruns the whole unit (with a cleared change tracker) after a transient failure.

## Several contexts

```csharp
twinbox
    .UseEntityFrameworkCore<OrdersContext>()    // also hosts the inbox
    .UseEntityFrameworkCore<BillingContext>();
```

Each context has its own outbox table and store, and messages go to the table of the context that saved them. The
inbox lives in the first context, so handlers that write to another context aren't in the inbox's transaction.

## Limitations

- SQLite is fine for development and tests, but it serializes writers: don't run many dispatching instances against it.
- MySQL needs 8.0 or later for `SKIP LOCKED`.
