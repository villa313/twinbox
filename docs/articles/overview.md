# Overview

A service that writes to its database and then publishes to a broker can fail between the two. It either loses the
message or publishes something that never committed. Twinbox fixes both directions:

- **Outbox.** `IOutbox.Send` buffers a message. It is written to an outbox table in the same transaction as your data,
  and a background dispatcher delivers it after the commit, with retries.
- **Inbox.** A received message runs its handler inside one transaction together with an inbox entry keyed by
  message id and handler. A redelivered message finds the entry and is skipped.

You keep your own `DbContext`, `DbConnection` or `IClientSessionHandle`. Twinbox adds an `IOutbox` you inject and an
`IHandle<T>` you implement. There are no base classes and no bus abstraction to program against.

## How a message flows

1. Your code calls `outbox.Send(message)` inside a unit of work.
2. The message is saved in the outbox table when the unit of work commits (`SaveChanges`, `outbox.CommitAsync(transaction)`
   or `outbox.CommitAsync(session)`).
3. The dispatcher is woken, leases due rows (`SKIP LOCKED` / `READPAST`, so many instances can run), and sends each
   one through the transport named by its route.
4. On the receiving side, a transport listener hands the message to the inbound pipeline, which deserializes it,
   records it in the inbox and runs your handler in the same transaction.
5. Messages the handler sends are saved in that same transaction, so a whole chain is exactly-once in effect.

## Packages

Most apps install two packages, a store for their database and a transport for their broker; the core `Twinbox`
package comes with them. The [home page](../index.md#which-packages-do-i-need) maps each database and broker to its
package and setup call, and `dotnet new install Twinbox.Templates` followed by
`dotnet new twinbox --store <store> --transport <transport>` generates a working app for any pair.

| Package | What it adds |
|---|---|
| `Twinbox` | The core: `AddTwinbox`, dispatcher, inbox pipeline, retries, routing, tenancy, health checks, and the [in-memory store and loopback transport](stores/in-memory.md) for tests and local development. |
| `Twinbox.Abstractions` | `IOutbox`, `IHandle<T>`, `IHandleBatch<T>`, `MessageContext`, `SendOptions`, filters. No dependencies, for message and handler libraries. |
| `Twinbox.EntityFrameworkCore` | Store in your `DbContext` (SQL Server, PostgreSQL, MySQL, Oracle, SQLite). |
| `Twinbox.SqlServer`, `Twinbox.PostgreSql`, `Twinbox.MySql`, `Twinbox.Oracle` | Stores for Dapper and plain ADO.NET. |
| `Twinbox.MongoDB` | Store for MongoDB (replica set required for transactions). |
| `Twinbox.InMemory` | Compatibility only: the in-memory store and transport are now part of `Twinbox`. |
| `Twinbox.AzureServiceBus`, `Twinbox.EventHubs`, `Twinbox.AmazonSqs`, `Twinbox.GooglePubSub`, `Twinbox.RabbitMQ`, `Twinbox.Kafka`, `Twinbox.Nats`, `Twinbox.RedisStreams`, `Twinbox.Pulsar` | Broker transports. |
| `Twinbox.Http` | Sends messages as HTTP requests: vendor API calls and outgoing webhooks. |
| `Twinbox.Webhooks` | Signed webhook ingress: verify, store, answer 200, handle later. |
| `Twinbox.AzureFunctions` | Dispatch from a timer and feed Service Bus triggers through the inbox. |
| `Twinbox.Dashboard` | Operations page and JSON API: stats, browsing, replay and removal of dead messages. |
| `Twinbox.Aspire`, `Twinbox.Aspire.Hosting` | Service defaults (OpenTelemetry, health check) and AppHost integration. |
| `Twinbox.Testing` | Deterministic test harness and store conformance suites. |
| `Twinbox.Templates` | `dotnet new twinbox`: an ASP.NET Core app wired with the store and transport you pick. |

All packages target .NET 8 and .NET 10 and are trimming and Native AOT compatible. A source generator in the core
package registers handlers without reflection.

## Next steps

- [Getting started with EF Core](getting-started-efcore.md)
- [Getting started with Dapper and ADO.NET](getting-started-ado.md)
- [Delivery guarantees](concepts/delivery-guarantees.md) explains exactly what Twinbox promises.
