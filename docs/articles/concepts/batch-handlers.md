# Batch handlers

`IHandleBatch<T>` receives several messages of one type at once, for bulk inserts or calls to APIs that take lists.

```csharp
public class StoreReadings(TelemetryContext db) : IHandleBatch<SensorReading>
{
    public Task HandleAsync(IReadOnlyList<BatchItem<SensorReading>> batch, CancellationToken ct)
    {
        db.Readings.AddRange(batch.Select(item => new Reading(item.Message.SensorId, item.Message.Value)));
        return Task.CompletedTask;   // saved and committed together with the inbox entries
    }
}

twinbox.AddBatchHandler<StoreReadings, SensorReading>();
```

Each `BatchItem<T>` carries the message and its `MessageContext`.

Batch handler calls go through [`IBatchMessageFilter`s](filters.md#batches-ibatchmessagefilter), not
`IMessageFilter`s, whatever the batch size.

## Where batches come from

Batches are only as large as the transport delivers them:

| Transport | Batch size |
|---|---|
| [Kafka](../transports/kafka.md) | Up to `MaxBatchSize` records of one partition, waiting up to `MaxBatchWait` to fill (default size 1) |
| [Event Hubs](../transports/event-hubs.md) | Up to `MaxBatchSize` events of one partition (default 1) |
| Every other transport | 1 |

Within a received batch, messages are grouped by type and tenant; each group goes to the batch handler in one call.
A plain `IHandle<T>` registered for the same type still gets the messages one at a time.

## Deduplication and transactions

With a store that supports batch inboxes (EF Core, the ADO.NET stores, in-memory), the batch runs in one transaction:
inbox entries are inserted for every message, the ones already processed are dropped, and the handler is called once
with the rest. If nothing is new, the handler isn't called. The whole batch commits or rolls back together.

MongoDB's inbox handles one message at a time, so a batch handler on MongoDB receives batches of one.

## Failures

A failed batch rolls back as a unit. Kafka and Event Hubs then retry the records one by one, so a single bad message
can't sink its neighbours again, and only that record waits for its retry.
