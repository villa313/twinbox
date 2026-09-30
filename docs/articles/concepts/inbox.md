# The inbox

Brokers deliver at least once, so a handler can see the same message twice. The inbox makes that harmless: before a
handler runs, Twinbox inserts an entry keyed by **(message id, consumer name)** in the same transaction as the
handler's own writes and the messages it sends. If the entry already exists, the handler is skipped and the message
is acknowledged.

```mermaid
sequenceDiagram
    participant Broker
    participant Pipeline as Inbound pipeline
    participant DB as Database
    Broker->>Pipeline: message (id 42)
    Pipeline->>DB: begin; insert inbox (42, ShipOrder)
    Pipeline->>Pipeline: run ShipOrder
    Pipeline->>DB: save handler writes + outgoing messages; commit
    Pipeline-->>Broker: ack
    Broker->>Pipeline: redelivery (id 42)
    Pipeline->>DB: insert inbox (42, ShipOrder) finds a duplicate
    Pipeline-->>Broker: ack, handler skipped
```

If the handler throws, the transaction rolls back: the inbox entry, the handler's writes and its outgoing messages all
disappear, and the broker redelivers.

## Per-handler deduplication

Each handler is deduplicated on its own. When two handlers handle `OrderPlaced` and one fails, the redelivery skips
the one that succeeded and retries only the other.

The consumer name defaults to the handler's namespace-qualified type name (generic arguments included, without
assembly versions). Renaming or moving the handler class changes it, and messages already processed under the old
name would run again. Pin it when you register:

```csharp
twinbox.AddHandler<ShipOrder, OrderPlaced>(consumerName: "shipping.ship-order");
```

## Writing from a handler

Handler writes must go through the inbox's transaction:

| Store | How the handler writes |
|---|---|
| EF Core | Inject your `DbContext`. The inbox begins the transaction and calls `SaveChanges` after the handler. |
| SQL Server, PostgreSQL, MySQL, Oracle | Inject `HandlerTransaction` and use `tx.Connection` with `tx.Transaction`. |
| MongoDB | Inject `MongoHandlerSession` and pass `session.Session` to every write; use `session.Database`. |
| In-memory | Nothing to do; handler failures discard the messages it sent. |

## Messages the inbox can't process

| Situation | Result |
|---|---|
| No message id (and no [header profile](headers.md) supplies one) | `PermanentDeliveryException`: dead-lettered by the transport. |
| No type registered for the message name, or no handler for it | Dead-lettered by default. Set `Twinbox:Inbox:UnknownMessages` to `Ignore` to acknowledge and log instead. |
| Body can't be deserialized | Dead-lettered. |
| Handler throws `PermanentDeliveryException` | Dead-lettered without further redelivery. |
| Handler throws anything else | Rolled back and redelivered by the broker. |

What "dead-lettered" means depends on the transport (a dead-letter queue, topic, stream or subject); see its page.

## How long duplicates are detected

Inbox entries are purged after `Twinbox:Retention:InboxEntries` (7 days by default). A redelivery older than that
runs the handler again. Keep the window longer than the broker's longest possible redelivery delay. See
[Retention](retention.md).

## Turning the inbox off

`Twinbox:Inbox:Enabled = false` runs handlers without an inbox entry and without a surrounding transaction. Only do
that when every handler is idempotent on its own.

## Batch handlers

`IHandleBatch<T>` handlers are deduplicated too: already-processed messages are filtered out before the call, and the
batch commits as one transaction. See [Batch handlers](batch-handlers.md).
