# Stores

A store keeps the outbox and inbox tables (or collections) and implements claiming, completion, purging and
browsing. Pick the one that matches how your app already talks to its database.

| Store | Package | Your code saves messages with | Handlers write through | Schema |
|---|---|---|---|---|
| [EF Core](entity-framework-core.md) | `Twinbox.EntityFrameworkCore` | `SaveChanges` | Your `DbContext` | Your EF migrations (`modelBuilder.AddTwinbox()`) |
| [SQL Server](sql-server.md) | `Twinbox.SqlServer` | `outbox.CommitAsync(transaction)` | `HandlerTransaction` | Created on startup, or yours |
| [PostgreSQL](postgresql.md) | `Twinbox.PostgreSql` | `outbox.CommitAsync(transaction)` | `HandlerTransaction` | Created on startup, or yours |
| [MySQL](mysql.md) | `Twinbox.MySql` | `outbox.CommitAsync(transaction)` | `HandlerTransaction` | Created on startup, or yours |
| [Oracle](oracle.md) | `Twinbox.Oracle` | `outbox.CommitAsync(transaction)` | `HandlerTransaction` | Created on startup, or yours |
| [MongoDB](mongodb.md) | `Twinbox.MongoDB` | `outbox.CommitAsync(session)` | `MongoDBHandlerSession` | Indexes created on startup |
| [In-memory](in-memory.md) | `Twinbox.InMemory` | `InMemoryUnitOfWork.CommitAsync()` | Nothing to do | None |

All built-in stores:

- let several instances dispatch at once without an external lock (row leasing with `SKIP LOCKED`, `READPAST`, or
  atomic find-and-modify);
- keep per-partition-key order (only the oldest unsent message of a key is claimable);
- support [multi-tenancy](../concepts/multi-tenancy.md) through per-scope connection strings or database names;
- implement `IOutboxAdmin`, so the [dashboard](../dashboard.md) can browse, replay and delete messages;
- pass the conformance suites in `Twinbox.Testing` (see [Testing](../testing.md#store-conformance)).

Batch deduplication for [batch handlers](../concepts/batch-handlers.md) is supported by every store except MongoDB,
which processes batches one message at a time.

## One store per app, usually

Register one store. EF Core is the exception: `UseEntityFrameworkCore<T>()` can be called for several contexts
(a modular monolith), and the dispatcher drains every context's outbox. The first context hosts the inbox.

Each store has a name (`IOutboxStore.Name`): `SqlServer`, `PostgreSql`, `MySql`, `Oracle`, `MongoDB`, `InMemory`, or
the context's class name for EF Core. Features that write to one store, the [webhook inbox](../webhooks.md) and the
[outbox import](../migration.md), use the only store registered, or the one you name; with several stores and no
name they fail at startup instead of guessing.

## Writing your own

Implement `Twinbox.Storage.IOutboxStore` (and `IInboxStore` for receiving; `IBatchInboxStore` and `IOutboxAdmin` are
optional), register it as a singleton, and run the conformance suites against it. Create DI scopes through
`TwinboxScopeFactory` so multi-tenancy keeps working.

Everything a store author needs is public in `Twinbox.Storage`; nothing relies on internals shared between packages.
Hooks that application code should never call are hidden from IntelliSense, for example
`HandlerTransactionBinding.Bind`, which an ADO.NET inbox uses to expose its connection and transaction to handlers
through `HandlerTransaction` while they run.
