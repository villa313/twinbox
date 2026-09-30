---
_layout: landing
---

# Twinbox

**Transactional outbox and inbox for .NET.** Save messages in the same transaction as your data, deliver them to any
broker, and deduplicate them on the way in. Works with EF Core, Dapper or plain ADO.NET, with no base classes and no
bus abstraction to adopt.

```csharp
builder.Services.AddDbContext<ShopContext>(o => o.UseSqlServer(connectionString));
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<ShopContext>()
    .UseAzureServiceBus(serviceBusConnectionString)
    .Route<OrderPlaced>().To("orders"));
```

```csharp
db.Orders.Add(order);
outbox.Send(new OrderPlaced(order.Id));   // IOutbox is injected
await db.SaveChangesAsync();              // the order and the message commit together
```

> Twinbox is in early development (0.1.0-alpha). APIs may still change.

## Where to start

- [Overview](articles/overview.md): what Twinbox does and which packages you need.
- [Getting started with EF Core](articles/getting-started-efcore.md) or [with Dapper / ADO.NET](articles/getting-started-ado.md).
- [Concepts](articles/concepts/outbox.md): the outbox, the inbox, ordering, retries and the rest.
- [Stores](articles/stores/index.md) and [transports](articles/transports/index.md): one page per database and broker.
- [Migrating from another outbox](articles/migration.md): move one service at a time.
- [API reference](api/Twinbox.yml).
