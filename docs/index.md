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

## Which packages do I need?

A typical app installs **two packages: one store and one transport**. The core `Twinbox` package comes with them.
The fastest start is the template, which generates a working app for any pair:

```bash
dotnet new install Twinbox.Templates
dotnet new twinbox --store efcore-postgres --transport rabbitmq
```

| Your database | Package | Setup call |
|---|---|---|
| EF Core (SQL Server, PostgreSQL, MySQL, Oracle, SQLite) | `Twinbox.EntityFrameworkCore` | `UseEntityFrameworkCore<TContext>()` |
| SQL Server with Dapper / ADO.NET | `Twinbox.SqlServer` | `UseSqlServer(connectionString)` |
| PostgreSQL with Dapper / ADO.NET | `Twinbox.PostgreSql` | `UsePostgreSql(connectionString)` |
| MySQL with Dapper / ADO.NET | `Twinbox.MySql` | `UseMySql(connectionString)` |
| Oracle with Dapper / ADO.NET | `Twinbox.Oracle` | `UseOracle(connectionString)` |
| MongoDB | `Twinbox.MongoDB` | `UseMongoDB(connectionString, databaseName)` |
| None yet (tests, demos) | built into `Twinbox` | `UseInMemoryStore()` |

| Your broker | Package | Setup call |
|---|---|---|
| Azure Service Bus | `Twinbox.AzureServiceBus` | `UseAzureServiceBus(connectionString)` |
| Azure Event Hubs | `Twinbox.EventHubs` | `UseEventHubs(connectionString)` |
| Amazon SQS / SNS | `Twinbox.AmazonSqs` | `UseAmazonSqs(region)` |
| Google Cloud Pub/Sub | `Twinbox.GooglePubSub` | `UseGooglePubSub(projectId)` |
| RabbitMQ | `Twinbox.RabbitMQ` | `UseRabbitMQ(connectionString)` |
| Kafka | `Twinbox.Kafka` | `UseKafka(bootstrapServers)` |
| NATS JetStream | `Twinbox.Nats` | `UseNats(url)` |
| Redis Streams | `Twinbox.RedisStreams` | `UseRedisStreams(configuration)` |
| Apache Pulsar | `Twinbox.Pulsar` | `UsePulsar(serviceUrl)` |
| HTTP APIs and outgoing webhooks | `Twinbox.Http` | `UseHttp(http => ...)` |
| None (in-process events) | built into `Twinbox` | `UseLocalDelivery()` |

Optional add-ons: [`Twinbox.Dashboard`](articles/dashboard.md), [`Twinbox.Webhooks`](articles/webhooks.md),
[`Twinbox.Testing`](articles/testing.md), [`Twinbox.Aspire` and `Twinbox.Aspire.Hosting`](articles/aspire.md),
[`Twinbox.AzureFunctions`](articles/azure-functions.md).

## Where to start

- [Overview](articles/overview.md): what Twinbox does and which packages you need.
- [Getting started with EF Core](articles/getting-started-efcore.md) or [with Dapper / ADO.NET](articles/getting-started-ado.md).
- [Concepts](articles/concepts/outbox.md): the outbox, the inbox, ordering, retries and the rest.
- [Stores](articles/stores/index.md) and [transports](articles/transports/index.md): one page per database and broker.
- [Migrating from another outbox](articles/migration.md): move one service at a time.
- [API reference](api/Twinbox.yml).
