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

The receiver must still know the concrete type by its wire name (`OrderPlaced` here), including after a restart,
before it has sent one itself. A route or handler for the exact type registers the name; for a whole hierarchy, register
every concrete subtype at once:

```csharp
twinbox.AddHandler<AuditTrail, IDomainEvent>()
    .AddSubtypesOf<IDomainEvent>();                 // scans IDomainEvent's assembly; pass others to widen it
```

`AddSubtypesOf` scans with reflection, so trimmed apps should register each type instead. A test can check that
nothing slipped through with `FindUnhandledSubtypes<TBase>()` from `Twinbox.Testing` (see [Testing](../testing.md)).

## Several handlers for one message

Each handler runs in its own scope and transaction, in registration order, and is deduplicated separately. If the
second handler fails, the first isn't run again on redelivery.

## Failing on purpose

Throw `Twinbox.Transport.PermanentDeliveryException` when retrying can't help (bad data, a business rule that will
never pass). The message is dead-lettered instead of redelivered. Any other exception means "try again".

## Local delivery

`UseLocalDelivery()` adds a transport named `"local"` that calls your own handlers from the dispatcher. It gives you
durable, retried in-process events with no broker. See [Local delivery](../transports/local.md).

### Handing domain events to an in-process mediator

An app that already handles domain events with a mediator (MediatR's `INotificationHandler`, for example) can keep
those handlers and let Twinbox make the events durable. One polymorphic handler forwards every event:

```csharp
public sealed class MediatorRelay(IPublisher publisher) : IHandle<DomainEvent>
{
    // Publish dispatches on the runtime type, so each event reaches its own notification handlers.
    public Task HandleAsync(DomainEvent message, MessageContext context, CancellationToken ct) =>
        publisher.Publish(message, ct);
}

builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseLocalDelivery()
    .Route<DomainEvent>().To("domain-events", transport: "local")
    .AddHandler<MediatorRelay, DomainEvent>(consumerName: "domain-events.mediator")
    .AddSubtypesOf<DomainEvent>());
```

Raise events by calling `IOutbox.Send` in the same unit of work as the change (for EF Core, before
`SaveChangesAsync`). Things to know:

- **One inbox entry covers every notification handler** of an event, because Twinbox sees a single consumer. If one
  of them throws, the whole delivery is retried and the others run again, so keep them idempotent or give each its
  own Twinbox handler instead.
- **Give the relay a fixed `consumerName`.** It is the inbox key; a fixed name survives renaming the class.
- **Work a notification handler starts elsewhere** (a background job, an HTTP call) isn't covered by the inbox. See
  [Where the guarantee stops](delivery-guarantees.md#where-the-guarantee-stops).
