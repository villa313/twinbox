# Twinbox

**Transactional outbox & inbox for .NET.** The one you add, not the framework you adopt.

Twinbox saves your messages in the same transaction as your data, delivers them to any broker, and
deduplicates them on the way in. It works with EF Core, Dapper or plain ADO.NET. You don't need base classes,
a bus abstraction or a special transaction API.

> **Status:** early development (0.1.0-alpha). APIs may still change.

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
    .UseRabbitMq(o => o.ConnectionUri = new Uri(rabbitUri))
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

## Calling HTTP APIs and delivering webhooks

`Twinbox.Http` sends each message as an HTTP request, so a vendor API call or a webhook gets the outbox's
retries, backoff and circuit breaker instead of a hand-written dispatcher:

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

```json
{
  "Twinbox": {
    "Http": {
      "Endpoints": {
        "partner-webhook": {
          "Url": "https://partner.example/hooks/orders",
          "Method": "POST",
          "Timeout": "00:00:10",
          "WebhookSecret": "whsec_...",
          "TransientStatusCodes": [ 423 ]
        }
      }
    }
  }
}
```

- The body is the message payload with its content type, and `Idempotency-Key` carries the message id, so a
  retried request can be deduplicated. `traceparent` is always sent; `ForwardHeaders = true` adds the Twinbox
  headers too.
- URL placeholders: `{messageId}`, `{messageName}`, `{partitionKey}`, `{tenant}`, or any message header by name.
  Values are URL-escaped, and a message missing one is dead-lettered.
- 2xx is delivered. 408, 429 and 5xx, timeouts and network errors are retried, waiting at least as long as
  `Retry-After` asks. Other statuses are dead-lettered with the status and the start of the response body.
- `WebhookSecret` adds [Standard Webhooks](https://www.standardwebhooks.com/) signature headers, so receivers can
  verify requests with any Standard Webhooks library.
- Each endpoint uses the `IHttpClientFactory` client `twinbox-http:{name}`. Add auth handlers with
  `e.ConfigureHttpClient = c => c.AddHttpMessageHandler<VendorAuthHandler>()`, or with
  `services.AddHttpClient(HttpTransport.HttpClientName("partner-webhook"))` for endpoints defined in configuration.

## Features

- **Storage:** EF Core (SQL Server, PostgreSQL, MySQL, Oracle, SQLite), Dapper/ADO.NET (SQL Server, PostgreSQL, MySQL, Oracle), MongoDB, in-memory
- **Transports:** Azure Service Bus, Amazon SQS/SNS, RabbitMQ, Kafka, NATS JetStream, Redis Streams, Apache Pulsar, HTTP APIs and webhooks, local delivery, in-memory
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
- **Observability:** OpenTelemetry tracing and metrics (`TwinboxDiagnostics.SourceName`), and health checks
  (`AddHealthChecks().AddTwinbox()`)
- **Dead letters:** a policy for giving up, `IDeadLetterObserver` notifications, and configurable handling of
  unknown messages

Payloads are JSON with camelCase property names (`JsonSerializerDefaults.Web`). Pass your own
`JsonSerializerOptions` to `UseSerializer(new SystemTextJsonMessageSerializer(options))` if you need something else.

## Moving over from another outbox

You can switch one service at a time without a big-bang cutover:

```csharp
twinbox
    // Keep talking to services that haven't moved yet, in both directions.
    .UseHeaderProfile(HeaderProfile.Prefixed("legacy"))       // legacy-msg-id, legacy-msg-name
    // Drain messages the old outbox never sent.
    .ImportFromExistingOutbox(o =>
    {
        o.CreateConnection = _ => new SqlConnection(connectionString);
        o.SelectPending = "SELECT TOP (@batch) Id, Name, Content FROM old.Published WHERE Status = 'Scheduled'";
        o.MarkImported = "UPDATE old.Published SET Status = 'Migrated' WHERE Id = @id";
    })
    // Remember what the old inbox already processed, so redeliveries are skipped.
    .SeedInboxFromExisting(o =>
    {
        o.CreateConnection = _ => new SqlConnection(connectionString);
        o.SelectProcessed = "SELECT Id AS MessageId, 'MyApp.ShipOrder' AS Consumer FROM old.Received WHERE Status = 'Succeeded'";
    });
```

Give message types their old names with `[MessageName("old.name")]`. Imported rows get deterministic ids, so if
a row is imported twice, the inbox discards the repeat.

## Testing

```csharp
services.AddTwinbox(twinbox => twinbox.UseTestHarness().Route<OrderPlaced>().To("orders"));

var harness = provider.GetTwinboxHarness();
await harness.DrainAsync();                          // dispatches and delivers deterministically
Assert.Single(harness.Sent<OrderPlaced>());
```

Store authors can run `OutboxStoreConformance.Cases` from any test framework to check their store against the
storage contract.

## License

[MIT](LICENSE), forever.
