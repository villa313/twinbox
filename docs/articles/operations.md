# Operations

## Schema

| Store | Who creates the tables |
|---|---|
| EF Core | You, through EF migrations. `modelBuilder.AddTwinbox()` adds the tables to your model; `dotnet ef migrations add` picks them up. |
| SQL Server, PostgreSQL, MySQL, Oracle | Twinbox at startup, when `CreateSchemaIfMissing` is `true` (the default). |
| MongoDB | Collections are created on first write; indexes at startup when `CreateIndexes` is `true`. |

With automatic creation, the app's database user needs DDL rights (create table and index, and create schema for SQL
Server and PostgreSQL when `Schema` is set). Creation runs before the app starts serving, in every tenant database the
tenant list returns, and again for a tenant database the first time it is used. It only creates what's missing and
never alters existing tables.

To own the schema yourself (typical when the app user has no DDL rights):

1. Set `CreateSchemaIfMissing = false`.
2. Take the DDL from the store's page ([SQL Server](stores/sql-server.md#schema),
   [PostgreSQL](stores/postgresql.md#schema), [MySQL](stores/mysql.md#schema), [Oracle](stores/oracle.md#schema)) and
   add it to your migrations, adjusting names if you changed them.

The EF Core and ADO.NET stores share one table layout (with default names), so moving a service from one to the
other needs no data migration.

### Upgrading Twinbox

Check the release notes for schema changes when upgrading. With EF Core, a model change shows up in your next
migration; with the ADO.NET stores and `CreateSchemaIfMissing = false`, apply the new DDL before deploying.

## Running many instances

Every instance can send, dispatch, receive and clean up at the same time; no leader election or external lock is
needed.

- **Leasing.** The dispatcher leases the rows it claims for `Dispatcher:LeaseDuration`. Other instances skip leased
  rows. If an instance dies, its leases expire and another instance picks the rows up (they may be sent twice; the
  receiving inbox absorbs that).
- **`InstanceId`** identifies the lease owner and must be unique per process. The default
  (`{machine}-{pid}-{guid}`) is; only override it with something equally unique.
- **Circuit breakers** are per instance. Each instance discovers an outage on its own.
- **Retention** runs everywhere and skips rows another instance is deleting. Turn it off on all but one instance if
  you prefer (`Retention:Enabled = false`).
- **Consumers** scale by the broker's own mechanism: competing consumers on a queue, partitions per consumer group, and
  so on. See the transport pages.

### Splitting roles

A common layout is a web tier that only writes and a worker tier that dispatches and consumes:

On the web tier:

```json
{ "Twinbox": { "Dispatcher": { "Enabled": false }, "Retention": { "Enabled": false } } }
```

Messages saved by the web tier are then sent on the worker's next poll (up to `MaxPollInterval`, 30 s by default)
rather than immediately after the commit, because the commit's wake-up signal is in-process. Lower `MaxPollInterval`
on the workers if that matters, or leave dispatching on everywhere.

## Tuning

| Symptom | Knob |
|---|---|
| Throughput too low with a deep backlog | Raise `Dispatcher:BatchSize` (100) and `MaxDegreeOfParallelism` (processor count). Parallelism applies across destinations; one destination is sent sequentially within a batch. |
| Too many idle queries | Raise `Dispatcher:MaxPollInterval`. Commits wake the dispatcher anyway. |
| Messages sent twice under load | `LeaseDuration` is shorter than a batch takes. Raise it, or lower `BatchSize` or `SendTimeout`. |
| One slow destination delays others | Give it its own destination; lower its `SendTimeout`; tune its circuit breaker under `Destinations`. |
| A partition key stuck behind a failing message | Expected head-of-line blocking; the message dead-letters after `MaxAttempts`. Lower attempts for that destination, or fix and replay. |
| Outbox table grows | Lower `Retention:SentMessages`; check that cleanup runs somewhere (`Retention:Enabled`). |
| Inbox table grows | Lower `Retention:InboxEntries`, but keep it longer than your longest redelivery window. |

Keep `Dispatcher:SendTimeout` below `LeaseDuration`, and per-endpoint HTTP timeouts below `SendTimeout`.

On PostgreSQL, the outbox is an update-heavy table; default autovacuum settings are usually fine, but watch dead tuples
on very busy systems.

## Dead letters

1. Alert on them: the `twinbox.outbox.dead_lettered` metric, the health check's `dead` count, or an
   `IDeadLetterObserver`.
2. Look at `LastError` in the [dashboard](dashboard.md).
3. Fix the cause (a missing queue, a rejected payload, an outage past the retry budget).
4. Replay from the dashboard, its API, or the Aspire *Replay dead letters* command.

Dead messages are kept until you act unless `Retention:DeadMessages` is set.

## Shutdown

On shutdown the dispatcher stops claiming. A batch that is interrupted keeps its leases until they expire, and another
instance (or the next start) sends those rows. Listeners stop receiving and let in-flight handlers settle where the
broker allows it; anything left unacknowledged is redelivered and deduplicated by the inbox.

## Renames that matter

- **Message names** are part of the wire contract. Pin them with `[MessageName]` before renaming or moving a type.
- **Consumer names** are the inbox identity of a handler (namespace-qualified type name by default). Renaming a
  handler class without pinning its consumer name makes already-processed messages look new. Pass `consumerName` to
  `AddHandler` to pin it.
