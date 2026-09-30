# Twinbox

**Transactional outbox & inbox for .NET.** The one you add, not the framework you adopt.

Twinbox saves your messages in the same transaction as your data, delivers them to any broker, and
deduplicates them on the way in. It works with EF Core, Dapper or plain ADO.NET. You don't need base classes,
a bus abstraction or a special transaction API.

> **Status:** early development (0.1.0-alpha). APIs may still change.
>
> **Docs:** https://twinbox-dotnet.github.io/twinbox/

## Why

A service that writes to its database and then publishes to a broker can fail between the two. It either loses
the message or publishes something that never committed. The outbox pattern fixes this by writing the message to
the database in the same transaction and publishing it afterwards. The inbox pattern fixes the other side: it
makes processing a redelivered message safe.

## Quickstart (EF Core)

```csharp
builder.Services.AddDbContext<ShopContext>(o => o.UseSqlServer(connectionString));
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<ShopContext>()
    .UseAzureServiceBus(serviceBusConnectionString)
    .Route<OrderPlaced>().To("orders"));
```

```csharp
public class ShopContext(DbContextOptions<ShopContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddTwinbox();
}
```

Add a migration and you're done. Then use it like this:

```csharp
db.Orders.Add(order);
outbox.Send(new OrderPlaced(order.Id));   // IOutbox is injected
await db.SaveChangesAsync();              // order and message commit together, then the message is sent
```

The outbox tables follow your model's naming conventions (snake_case included). Messages sent from a scope are
saved by the next `SaveChanges` of a context resolved in that scope, pooled contexts included. For a context you
create yourself, call `context.EnlistOutbox(outbox)`.

## Quickstart (Dapper / ADO.NET)

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseSqlServer(connectionString)        // or .UsePostgreSql / .UseMySql / .UseOracle
    .UseRabbitMQ(o => o.ConnectionUri = new Uri(rabbitUri))
    .Route<OrderPlaced>().To("orders"));
```

```csharp
await using var transaction = await connection.BeginTransactionAsync();
await connection.ExecuteAsync("INSERT INTO orders ...", order, transaction);
outbox.Send(new OrderPlaced(order.Id));
await outbox.CommitAsync(transaction);     // saves the message, commits, wakes the dispatcher
```

Tables are created on startup. Set `CreateSchemaIfMissing = false` if your migrations own the schema.

## Handling messages

```csharp
public class ShipOrder(ShopContext db) : IHandle<OrderPlaced>
{
    public async Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        // Runs once per message id, even if the broker redelivers it.
        // Your writes, the inbox entry and any messages you send commit together.
    }
}
```

Register handlers with `AddHandler<ShipOrder, OrderPlaced>()`, or let the bundled source generator do it for
the whole assembly without reflection (Native AOT friendly):

```csharp
builder.Services.AddTwinbox(twinbox => twinbox.AddHandlersFromMyApp());
```

With Dapper, write through the handler's transaction:
`await tx.Connection.ExecuteAsync(sql, args, tx.Transaction)`, where `tx` is an injected `HandlerTransaction`.

## In-process events without a broker

`UseLocalDelivery()` has the dispatcher call your own handlers. You get durable, retried domain events with
nothing else to run:

```csharp
twinbox.UseLocalDelivery().Route<DomainEvent>().To("domain-events", transport: "local");
```

A route or handler registered for a base class or interface covers all of its subtypes.

## HTTP APIs and webhooks

`Twinbox.Http` sends messages as HTTP requests, so a vendor API call or an outgoing webhook gets the outbox's retries,
backoff, circuit breaker and an `Idempotency-Key`, plus optional [Standard Webhooks](https://www.standardwebhooks.com/)
signatures:

```csharp
twinbox
    .UseHttp(http => http.AddEndpoint("valuelink", e =>
    {
        e.Url = new Uri("https://api.vendor.com/tenants/{tenant}/orders");
        e.Headers["X-Api-Key"] = apiKey;
        e.TreatAsSuccess(409);   // the vendor answers 409 to a repeat it already applied
    }))
    .Route<OrderPlaced>().To("valuelink", transport: "http");
```

`Twinbox.Webhooks` receives them: it verifies the signature (Stripe, Shopify, GitHub, Standard Webhooks or any HMAC),
stores the webhook and answers 200 right away, then runs your handler with retries, once per provider event id.

```csharp
app.MapWebhookInbox<StripeEvent>("/webhooks/stripe",
    w => w.VerifyStripe(WebhookSecrets.FromConfiguration("Stripe:WebhookSecret")));
```

See [HTTP transport](https://twinbox-dotnet.github.io/twinbox/articles/transports/http.html) and
[Webhooks](https://twinbox-dotnet.github.io/twinbox/articles/webhooks.html).

## Features

- **Storage:** EF Core (SQL Server, PostgreSQL, MySQL, Oracle, SQLite), Dapper/ADO.NET (SQL Server, PostgreSQL, MySQL, Oracle), MongoDB, in-memory
- **Transports:** Azure Service Bus, Azure Event Hubs, Amazon SQS/SNS, Google Cloud Pub/Sub, RabbitMQ, Kafka, NATS JetStream, Redis Streams, Apache Pulsar, HTTP APIs and webhooks, local delivery, in-memory
- **Delivery:** at-least-once, plus an inbox for effectively-once processing that is deduplicated per handler
- **Retries:** exponential backoff with jitter and a circuit breaker per destination, configurable from
  `appsettings.json` (`Twinbox:Destinations:{name}:Retry`)
- **Ordering:** messages with the same partition key are delivered in order; everything else goes in parallel
- **Scaling out:** safe across instances through row leasing (`SKIP LOCKED` / `READPAST`), with no external lock
- **Batch handlers:** `IHandleBatch<T>` receives several messages at once, with duplicates already filtered out
- **Request/reply and correlation:** `SendOptions.ReplyTo` + `outbox.Reply(context, response)`; messages sent while
  handling one inherit its correlation id
- **Filters:** `IMessageFilter` wraps handler calls, `IOutgoingMessageFilter` stamps headers on outgoing messages
- **Interop:** header profiles for other systems' header names, and CloudEvents binary-mode headers
  (`HeaderProfile.CloudEvents("/my-service")`)
- **Delayed sends:** `new SendOptions { Delay = TimeSpan.FromMinutes(5) }`
- **Multi-tenancy:** a database per tenant through `UseTenants(...)`, plus several DbContexts per app
- **Observability:** OpenTelemetry tracing and metrics (`TwinboxDiagnostics.SourceName`), health checks
  (`AddHealthChecks().AddTwinbox()`), and .NET Aspire integration (`Twinbox.Aspire`, `Twinbox.Aspire.Hosting`)
- **Dead letters:** a policy for giving up, `IDeadLetterObserver` notifications, and configurable handling of
  unknown messages

Payloads are JSON with camelCase property names (`JsonSerializerDefaults.Web`). Pass your own
`JsonSerializerOptions` to `UseSerializer(new SystemTextJsonMessageSerializer(options))` if you need something else.

For Native AOT and trimmed apps, give the serializer a source-generated context covering your messages, and
register handlers with `AddHandler<THandler, TMessage>()` or the generated `AddHandlersFrom...()`:

```csharp
[JsonSerializable(typeof(OrderPlaced))]
internal sealed partial class AppJsonContext : JsonSerializerContext;

builder.Services.AddTwinbox(twinbox => twinbox
    .UseJsonTypeInfoResolver(AppJsonContext.Default)
    .AddHandlersFromMyApp());
```

`samples/Twinbox.AotSmoke` is published with Native AOT in CI. Twinbox.EntityFrameworkCore isn't AOT compatible. See [Native AOT](https://twinbox-dotnet.github.io/twinbox/articles/concepts/serialization.html).

## Hosting and operations

- **Azure Functions:** `UseAzureFunctions()` hands dispatching, cleanup and Service Bus triggers to your own functions.
  [Guide](https://twinbox-dotnet.github.io/twinbox/articles/azure-functions.html)
- **Dashboard:** `app.MapTwinboxDashboard("/twinbox").RequireAuthorization("ops")` serves stats, message browsing and
  dead-letter replay. It refuses every request until an authorization policy is attached.
  [Guide](https://twinbox-dotnet.github.io/twinbox/articles/dashboard.html)
- **.NET Aspire:** `builder.AddTwinboxServiceDefaults()` in ServiceDefaults and `.WithTwinboxDashboard("/twinbox")` in
  the AppHost. [Guide](https://twinbox-dotnet.github.io/twinbox/articles/aspire.html)

## Moving over from another outbox

Switch one service at a time: `UseHeaderProfile(...)` keeps the wire format compatible in both directions,
`ImportFromExistingOutbox(...)` drains messages the old outbox never sent, and `SeedInboxFromExisting(...)` stops
redeliveries of already-processed messages from running again. See the
[migration guide](https://twinbox-dotnet.github.io/twinbox/articles/migration.html).

## Testing

```csharp
services.AddTwinbox(twinbox => twinbox.UseTestHarness().Route<OrderPlaced>().To("orders"));

var harness = provider.GetTwinboxHarness();
await harness.DrainAsync();                          // dispatches and delivers deterministically
Assert.Single(harness.Sent<OrderPlaced>());
```

Store authors can run `OutboxStoreConformance.Cases` and `OutboxAdminConformance.Cases` from any test framework.
[Guide](https://twinbox-dotnet.github.io/twinbox/articles/testing.html)

## Documentation

Full documentation, one page per store and transport, and the API reference:
**https://twinbox-dotnet.github.io/twinbox/**

## License

[MIT](LICENSE), forever.
