# Getting started with EF Core

This walkthrough uses SQL Server and Azure Service Bus. Any [store](stores/index.md) and
[transport](transports/index.md) works the same way.

## 1. Install

```bash
dotnet add package Twinbox
dotnet add package Twinbox.EntityFrameworkCore
dotnet add package Twinbox.AzureServiceBus
```

## 2. Add the tables to your model

```csharp
public class ShopContext(DbContextOptions<ShopContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddTwinbox();
}
```

`AddTwinbox()` maps an outbox and an inbox table. They follow your naming conventions (snake_case included). Pass
options to change the schema or table names:

```csharp
modelBuilder.AddTwinbox(o =>
{
    o.Schema = "messaging";
    o.OutboxTable = "Outbox";
    o.InboxTable = "Inbox";
});
```

Then add a migration as usual:

```bash
dotnet ef migrations add AddTwinbox
dotnet ef database update
```

## 3. Register Twinbox

```csharp
builder.Services.AddDbContext<ShopContext>(o => o.UseSqlServer(connectionString));

builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<ShopContext>()
    .UseAzureServiceBus(serviceBusConnectionString)
    .Route<OrderPlaced>().To("orders"));
```

Call `AddDbContext` before `UseEntityFrameworkCore`; Twinbox wraps that registration.

## 4. Send

```csharp
public record OrderPlaced(Guid OrderId);

app.MapPost("/orders", async (Order order, ShopContext db, IOutbox outbox) =>
{
    db.Orders.Add(order);
    outbox.Send(new OrderPlaced(order.Id));
    await db.SaveChangesAsync();   // order and message commit together, then the dispatcher is woken
    return Results.Created($"/orders/{order.Id}", order);
});
```

Messages sent from a DI scope are saved by the next `SaveChanges` of a Twinbox-enabled context resolved in that scope,
pooled contexts included. With an explicit transaction, the messages are written by `SaveChanges` and dispatched once
the transaction commits:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync();
db.Orders.Add(order);
outbox.Send(new OrderPlaced(order.Id));
await db.SaveChangesAsync();
await transaction.CommitAsync();
```

For a context you create yourself (not resolved from DI), enlist it once:

```csharp
await using var db = new ShopContext(options);
db.EnlistOutbox(outbox);
```

If a scope ends with messages that were sent but never saved, Twinbox logs an error naming the fix.

## 5. Handle

```csharp
public class ShipOrder(ShopContext db) : IHandle<OrderPlaced>
{
    public async Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        var order = await db.Orders.FindAsync([message.OrderId], ct);
        order!.Status = OrderStatus.Shipping;
        // No SaveChanges needed: the inbox saves and commits the handler's changes,
        // the inbox entry and any messages sent here in one transaction.
    }
}
```

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<ShopContext>()
    .UseAzureServiceBus(serviceBusConnectionString, o => o.Listen("orders"))
    .AddHandler<ShipOrder, OrderPlaced>());
```

The handler runs once per message id. A redelivery is skipped. See [Handlers](concepts/handlers.md) for registration
options, including the source generator.

> [!NOTE]
> The inbox runs inside the provider's execution strategy. With a retrying strategy (for example
> `EnableRetryOnFailure`), a transient failure can run the handler again after its transaction rolled back, so
> keep side effects outside the database idempotent or send them as messages.

## Several DbContexts

Call `UseEntityFrameworkCore<T>()` once per context. Each context gets its own outbox store, and the dispatcher drains
all of them. The first registered context also hosts the inbox.

```csharp
twinbox
    .UseEntityFrameworkCore<OrdersContext>()
    .UseEntityFrameworkCore<BillingContext>();
```

## Next

- [EF Core store details](stores/entity-framework-core.md): supported providers, table layout.
- [Delivery guarantees](concepts/delivery-guarantees.md).
- [Testing](testing.md) without a database or broker.
