# Local delivery

Part of the core `Twinbox` package · transport name `"local"`

`UseLocalDelivery()` adds a transport that hands messages straight to this app's own handlers. You get durable,
retried in-process events (domain events, background work) with nothing else to run: the outbox is the queue.

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<ShopContext>()
    .UseLocalDelivery()
    .Route<OrderPlaced>().To("order-events")
    .AddHandler<SendConfirmationEmail, OrderPlaced>()
    .AddHandler<UpdateSalesReport, OrderPlaced>());
```

With a broker registered as well, name the transport on local routes:

```csharp
twinbox
    .UseRabbitMQ(ConfigureRabbit)
    .UseLocalDelivery()
    .Route<IDomainEvent>().To("domain-events", transport: "local")
    .Route<OrderPlaced>().To("orders", transport: "rabbitmq");
```

## How it works

1. `outbox.Send` saves the message with your data, like any other.
2. The dispatcher claims it and "sends" it by running the inbound pipeline in-process: every handler for the type runs
   with the inbox, in its own scope and transaction.
3. If a handler throws, the send fails and the outbox retries it with backoff; handlers that already succeeded are
   skipped on the retry thanks to the inbox. After `Retry:MaxAttempts` the message is dead in the outbox.

The destination name is only a label; it appears as `MessageContext.Source` and in the dashboard, and it is the key
for [per-destination retry settings](../concepts/retries-and-dead-letters.md#per-destination-settings).

## Things to know

- Handlers run on the dispatcher, so a slow handler holds up its destination's group within a batch. Give heavy work
  its own destination so it doesn't delay other events.
- `Dispatcher:SendTimeout` (30 s) caps a delivery, handlers included. A handler that needs longer should be split up
  or the timeout raised.
- `PermanentDeliveryException` from a handler dead-letters the message immediately.
- Unknown message names and missing handlers dead-letter the message, as with any transport.
- [Webhook ingress](../webhooks.md) uses local delivery; `AddWebhooks()` turns it on.
