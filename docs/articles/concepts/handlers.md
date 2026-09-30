# Handlers

A handler implements `IHandle<TMessage>`. It is resolved from a fresh DI scope for every message, so it can take
scoped dependencies such as a `DbContext`.

```csharp
public class ShipOrder(ShopContext db, IOutbox outbox) : IHandle<OrderPlaced>
{
    public async Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        // context.MessageId, context.MessageName, context.Source (queue, topic...), context.Headers,
        // context.DeliveryAttempt, context.PartitionKey, context.CorrelationId, context.ReplyTo
    }
}
```

## Registering handlers

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .AddHandler<ShipOrder, OrderPlaced>()                        // one message type
    .AddHandler<ShipOrder, OrderPlaced>("shipping.ship-order")   // with a pinned consumer name
    .AddBatchHandler<ImportRows, RowImported>());                // IHandleBatch<T>
```

`AddHandler<THandler>()` registers a handler for every `IHandle<T>` it implements. It uses reflection, so it isn't
available under Native AOT or trimming; prefer the generated registration below.

### Source-generated registration

The `Twinbox` package includes a source generator. For each project that references it, it emits an
`AddHandlersFrom{AssemblyName}()` extension that registers every `IHandle<T>` and `IHandleBatch<T>` in the assembly,
with no reflection. The assembly name is PascalCased, so `MyApp.Orders` becomes `AddHandlersFromMyAppOrders()`.

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<ShopContext>()
    .AddHandlersFromMyAppOrders());
```

Generic and abstract handlers are skipped and reported with the info diagnostic `TWBX001`; register their closed types
with `AddHandler<THandler, TMessage>()`.

### Modules

`AddTwinbox` can be called several times. Later calls extend the first, so each module can register its own routes and
handlers:

```csharp
builder.Services.AddTwinbox(t => t.UseEntityFrameworkCore<AppContext>().UseRabbitMQ(ConfigureRabbit));
builder.Services.AddTwinbox(t => t.AddHandlersFromOrdersModule().Route<OrderPlaced>().To("orders"));
builder.Services.AddTwinbox(t => t.AddHandlersFromBillingModule());
```

## Polymorphic handlers

A handler registered for a base class or an interface receives every subtype:

```csharp
public interface IDomainEvent;
public record OrderPlaced(Guid OrderId) : IDomainEvent;

public class AuditTrail : IHandle<IDomainEvent> { /* sees OrderPlaced too */ }
```

The receiver must still know the concrete type by its wire name (`OrderPlaced` here). A route or handler for it
registers the name; otherwise call `IMessageNames.GetName(typeof(OrderPlaced))` at startup.

## Several handlers for one message

Each handler runs in its own scope and transaction, in registration order, and is deduplicated separately. If the
second handler fails, the first isn't run again on redelivery.

## Failing on purpose

Throw `Twinbox.Transport.PermanentDeliveryException` when retrying can't help (bad data, a business rule that will
never pass). The message is dead-lettered instead of redelivered. Any other exception means "try again".

## Local delivery

`UseLocalDelivery()` adds a transport named `"local"` that calls your own handlers from the dispatcher. It gives you
durable, retried in-process events with no broker. See [Local delivery](../transports/local.md).
