# The outbox

`IOutbox` is a scoped service. `Send` does not talk to a broker: it serializes the message, resolves its
[routes](routing.md) and buffers one outbox row per route. The rows are written when your unit of work commits, and
the dispatcher delivers them afterwards.

```csharp
public interface IOutbox
{
    void Send<TMessage>(TMessage message, SendOptions? options = null) where TMessage : class;
}
```

## Saving the buffered messages

| Store | How messages are saved |
|---|---|
| EF Core | Automatically by `SaveChanges` of a Twinbox-enabled context resolved in the same scope. Call `context.EnlistOutbox(outbox)` for contexts you create yourself. |
| SQL Server, PostgreSQL, MySQL, Oracle | `await outbox.CommitAsync(transaction)` or `await outbox.SaveAsync(transaction)`. |
| MongoDB | `await outbox.CommitAsync(session)` or `await outbox.SaveAsync(session)`. |
| In-memory | `InMemoryUnitOfWork.CommitAsync()`. |
| Inside a handler | Automatically, in the handler's inbox transaction. |

If a scope is disposed while messages are still buffered, Twinbox logs an error: they were sent but never saved.

## Send options

```csharp
outbox.Send(new OrderPlaced(order.Id), new SendOptions
{
    PartitionKey = order.CustomerId.ToString(),        // ordered per key
    Delay = TimeSpan.FromMinutes(5),                   // not before five minutes from now
    Headers = new Dictionary<string, string> { ["source"] = "checkout" },
    CorrelationId = requestId,                         // defaults to the handled message's correlation
    ReplyTo = "checkout-replies",                      // where the receiver should reply
});
```

| Option | Meaning |
|---|---|
| `PartitionKey` | Messages sharing a key are delivered in order. See [Ordering](ordering.md). |
| `Delay` | The row becomes due after this delay. See [Delayed send](delayed-send.md). |
| `Headers` | Extra headers sent with the message. See [Headers](headers.md). |
| `Destination`, `Transport` | Send to this address instead of the type's routes, e.g. a reply queue. |
| `ReplyTo` | Stored as the `twinbox-reply-to` header. See [Correlation and request/reply](correlation-and-reply.md). |
| `CorrelationId` | Stored as `twinbox-correlation-id`. |

## The dispatcher

A hosted service runs the dispatcher loop on every instance:

1. Claim up to `Dispatcher:BatchSize` due rows (100) with a lease of `Dispatcher:LeaseDuration` (30 s), skipping rows
   other instances hold (`FOR UPDATE SKIP LOCKED`, `READPAST`, or an atomic find-and-modify on MongoDB).
2. Group them by transport and destination, and send the groups in parallel (up to `Dispatcher:MaxDegreeOfParallelism`,
   the processor count by default). Within a group, messages are sent one at a time in claim order.
3. Record each outcome: sent, rescheduled with backoff, or dead.

A commit wakes the dispatcher right away. When there is nothing to do it backs off from `MinPollInterval` (1 s) to
`MaxPollInterval` (30 s). A full batch is followed immediately by the next one.

Each send is capped by `Dispatcher:SendTimeout` (30 s). A timeout counts as a failed attempt. If an instance dies
mid-batch, its leases expire and another instance picks the rows up again, so a message can be sent twice; the
receiving [inbox](inbox.md) absorbs that. A graceful shutdown is gentler: the messages already sent are recorded as
sent, and the rest of the batch is released right away instead of waiting for the lease to expire.

To dispatch from somewhere other than the background service (a timer function, a scheduled job, a test), set
`Twinbox:Dispatcher:Enabled` to `false` and call `ITwinboxMaintenance.DispatchPendingAsync(budget, ct)`, which
dispatches until the outbox is empty or the budget runs out, or `IOutboxDispatcher.DispatchBatchAsync` for a single
batch. See [Azure Functions](../azure-functions.md).

## Message ids

Each outbox row gets a UUIDv7 id from `IMessageIdGenerator` (time-ordered, index friendly). Transports send it as the
message id, and receivers deduplicate on it. Register your own `IMessageIdGenerator` to change the scheme.

## Message names

The wire name of a message type is its class name, `OrderPlaced`, not the namespace-qualified name. Two types with
the same name are rejected with an exception as soon as both are registered or sent. Pin a stable name with `[MessageName]`, so renaming or moving the class doesn't
break receivers:

```csharp
[MessageName("orders.placed.v1")]
public record OrderPlaced(Guid OrderId);
```
