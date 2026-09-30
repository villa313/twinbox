# In-memory

The in-memory store keeps the outbox and inbox in process memory, and its loopback transport delivers messages to the
app's own handlers. Nothing survives a restart, so use it for tests, demos and local development only.

It's built into the `Twinbox` package: there's nothing extra to install. The separate `Twinbox.InMemory` package is
kept only so apps built against 1.0 keep working; its types now live in `Twinbox` under the same names and namespaces.
You can remove the reference; if you keep it, upgrade it together with `Twinbox`.

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseInMemory()                       // store + transport
    .Route<OrderPlaced>().To("orders")
    .AddHandler<ShipOrder, OrderPlaced>());
```

`UseInMemory(configure)` is `UseInMemoryStore()` plus `UseInMemoryTransport(configure)`; use them separately to pair
the in-memory store with a real broker, or a real store with the in-memory transport.

For tests, prefer [`UseTestHarness()`](../testing.md), which builds on it and makes delivery deterministic.

## Saving messages

There's no database transaction, so commit the scope's messages explicitly with `InMemoryUnitOfWork`:

```csharp
await using var scope = app.Services.CreateAsyncScope();
scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(orderId));
await scope.ServiceProvider.GetRequiredService<InMemoryUnitOfWork>().CommitAsync();
```

Messages sent from handlers are committed automatically when the handler succeeds and discarded when it throws.

## Options

| Option | Default | Description |
|---|---|---|
| `InMemoryTransportOptions.AutoDeliver` | `true` | Deliver sent messages to handlers in the background. Turn off to pump them yourself with `InMemoryTransport.DeliverAsync`. |

## The loopback transport

`InMemoryTransport` (name `"inmemory"`) queues sent messages and delivers them through the inbound pipeline.

| Member | Use |
|---|---|
| `Sent` | Every message sent, in order. |
| `DeadLetteredIncoming` | Messages whose delivery to handlers failed permanently, or failed 10 times. |
| `OnSend` | A callback run before each send; throw from it to simulate a broker outage. |
| `DeliverAsync(pipeline, ct)` | Deliver everything queued right now; returns how many deliveries were attempted. |
| `WaitForMessagesAsync(timeout, ct)` | Wait until something is queued. |

A failed delivery is re-queued for the next `DeliverAsync` round, up to 10 attempts.

## The store

`InMemoryOutboxStore` implements `IOutboxStore` and `IOutboxAdmin`, so the [dashboard](../dashboard.md) works with it.
`Snapshot()` returns every stored row, which is handy in assertions. `InMemoryInboxStore.HasProcessed(messageId,
consumer)` tells you whether a handler has processed a message.
