# Ordering and partition keys

Messages without a partition key are delivered in parallel, in no particular order. Messages that share a partition
key are delivered one after another, in the order they were saved.

```csharp
outbox.Send(new OrderPlaced(order.Id), new SendOptions { PartitionKey = order.Id.ToString() });
outbox.Send(new OrderPaid(order.Id), new SendOptions { PartitionKey = order.Id.ToString() });
```

## How the outbox keeps order

Rows get an increasing sequence number when they're saved. The dispatcher only claims the **oldest unsent row of each
partition key**; later rows with the same key wait until it is sent. So:

- A row being retried holds back every later row with its key (head-of-line blocking). That is the price of order.
- A row that is dead-lettered stops blocking, and the next row of its key goes out. Order is kept, but there is a
  gap. Replaying the dead row later puts it back at its original position: it goes before any rows of its key that are
  still pending, but after the ones already sent.
- A delayed row (`SendOptions.Delay`) with a key also holds back the rows behind it until it is due.
- The rule applies per outbox table, across destinations. A message routed to two destinations with a key produces
  two rows, and the second is sent after the first.
- Keys are compared as exact strings. An empty key means no key.

Unkeyed rows are never blocked by keyed ones, and different keys never block each other.

## Keeping order on the broker

Once sent, order depends on the broker and on how consumers are configured:

| Transport | The partition key becomes | Ordered receiving needs |
|---|---|---|
| [Azure Service Bus](../transports/azure-service-bus.md) | `SessionId` when `UseSessions` is on | Session-enabled entities and `UseSessions = true` |
| [Event Hubs](../transports/event-hubs.md) | The event's partition key | Nothing extra: a partition is processed in order |
| [Amazon SQS](../transports/amazon-sqs.md) | The FIFO message group id | A `.fifo` queue or topic |
| [Google Pub/Sub](../transports/google-pubsub.md) | The ordering key when `EnableMessageOrdering` is on | `EnableMessageOrdering = true` |
| [Kafka](../transports/kafka.md) | The record key | Nothing extra: a partition is processed in order |
| [RabbitMQ](../transports/rabbitmq.md) | A header only | `ConsumerConcurrency = 1` and a single consuming instance |
| [NATS JetStream](../transports/nats.md) | A header only | `ConsumerConcurrency = 1` and a single consuming instance |
| [Pulsar](../transports/pulsar.md) | The message key | A `KeyShared` (or exclusive) subscription |
| [Redis Streams](../transports/redis-streams.md) | A header only | A single consumer in the group |

See each transport's page for the details, including what a failed delivery does to order on the receiving side.

## Inbound

`MessageContext.PartitionKey` carries the key to the handler. Twinbox doesn't reorder inbound messages; it handles them
in the order the transport delivers them.
