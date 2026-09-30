# Apache Kafka

`Twinbox.Kafka` · transport name `"kafka"` (`KafkaTransport.TransportName`)

A route destination is a Kafka topic. Each outbox message becomes one record keyed by its partition key. On the
receiving side Twinbox runs one consumer per listened topic and consumer group, and commits a record's offset only after
the inbox has handled it.

## Setup

With bootstrap servers, and optionally a callback for everything else:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseKafka("localhost:9092", kafka => kafka.Listen("payments", "billing"))
    .Route<OrderPlaced>().To("orders"));
```

With options, security settings and a listener:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseKafka(options =>
    {
        options.BootstrapServers = "broker-1:9093,broker-2:9093";
        options.ClientId = "billing";
        options.ConfigureClient = client =>
        {
            client.SecurityProtocol = SecurityProtocol.SaslSsl;
            client.SaslMechanism = SaslMechanism.Plain;
            client.SaslUsername = username;
            client.SaslPassword = password;
        };
        options.DeadLetterTopic = "orders-dead-letter";
        options.Listen("orders", "billing");
    })
    .Route<OrderPlaced>().To("orders"));
```

## Receiving

`Listen(topic, groupId)` consumes a topic as a member of a consumer group. Instances with the same group id share the
topic's partitions. Each listener gets its own consumer on a dedicated thread.

The message `Source` is the topic name.

## Options

| Option | Default | Description |
|---|---|---|
| `BootstrapServers` | `""` | Comma-separated `host:port` list. Required. |
| `ClientId` | `"twinbox"` | Reported to the brokers. |
| `ConfigureClient` | `null` | Applied to the producer, consumers and admin client, for example SASL or TLS. |
| `ConfigureProducer` | `null` | Last say over the `ProducerConfig`. |
| `ConfigureConsumer` | `null` | Last say over each listener's `ConsumerConfig`. |
| `AutoCreateTopics` | `false` | Create missing topics (sent to, listened on, and the dead-letter topic) before first use. |
| `TopicPartitions` | `1` | Partitions for auto-created topics. Must be positive. |
| `TopicReplicationFactor` | `1` | Replication factor for auto-created topics. Must be positive. |
| `SendTimeout` | 30 seconds | How long a send may wait for broker acknowledgement, retries included. Must be positive. |
| `DeadLetterTopic` | `null` | Topic that receives copies of permanently failing records. `null` logs and skips them. Cannot be blank. |
| `AutoOffsetReset` | `Earliest` | Where a group without a committed offset starts reading. |
| `RetryDelay` | 1 second | First delay before a failed record is retried. Doubles per attempt. Must be positive. |
| `MaxRetryDelay` | 30 seconds | Cap on the retry delay. Cannot be shorter than `RetryDelay`. |
| `MaxBatchSize` | `1` | Most records of one partition handed to the pipeline at once. Must be positive. |
| `MaxBatchWait` | 50 ms | How long to wait for more records to fill a batch. Only used when `MaxBatchSize` is above 1. Cannot be negative. |

The producer is idempotent with `acks=all`. Consumers run with auto-commit off. `ConfigureProducer` and
`ConfigureConsumer` run after these defaults and after `ConfigureClient`, so they can override them.

## Message mapping

| Twinbox | Kafka |
|---|---|
| Message id | header `twinbox-message-id` |
| Message name | header `twinbox-message-name` |
| Content type | header `content-type` |
| Partition key | record key |
| Headers | headers (UTF-8) |

On receive, a record without `twinbox-message-id` gets its coordinates `<topic>:<partition>:<offset>` as id. They are
stable across redeliveries, so records from other producers are still deduplicated. The partition key is the record
key. A missing content type becomes `application/octet-stream`. Records without a `twinbox-message-name` header need a
[header profile](../concepts/headers.md) that supplies the name.

## Failures, retries and dead letters

Sending: these error codes are permanent and dead-letter the outbox row: `UnknownTopicOrPart`, `Local_UnknownTopic`,
`TopicException`, `MsgSizeTooLarge`, `InvalidConfig` and `Local_InvalidArg`. Anything else is retried by the outbox,
including authorization failures, so fixing an ACL releases the backlog instead of finding it dead-lettered. A fatal producer error replaces the producer. See
[Retries and dead letters](../concepts/retries-and-dead-letters.md).

Receiving: Kafka has no per-record acknowledgement, so Twinbox retries in place.

- Handler succeeds: the offset is committed synchronously.
- Any other exception: only that partition is paused and rewound to the record, then retried after `RetryDelay`,
  doubling up to `MaxRetryDelay`. There is no attempt limit. Other partitions keep flowing.
- `PermanentDeliveryException`: the record is copied to `DeadLetterTopic` with its key, value and headers, plus
  `twinbox-error` and `twinbox-origin` (`<topic>:<partition>:<offset>`), then committed past. Without a dead-letter
  topic it is logged at error level, counted in `twinbox.inbox.discarded` and committed past (see
  [the transports overview](index.md#permanent-failures-without-a-dead-letter-destination)). If the copy fails, the
  record is retried like a handler failure.

The delivery attempt is counted in memory and resets after a restart or rebalance. A failed commit only means the record
may be read again, which the inbox deduplicates.

With `MaxBatchSize` above 1, records of one partition are collected for up to `MaxBatchWait` and handed over as one
batch. A failed batch is retried record by record so the failing record can be isolated. See
[Batch handlers](../concepts/batch-handlers.md).

## Ordering

The partition key is the record key, so the producer's partitioner puts one key on one partition. Each partition is
processed in offset order, and a failing record blocks the records behind it until it succeeds or is dead-lettered,
so per-key order holds end to end. Messages without a partition key have a null key and no order. See
[Ordering](../concepts/ordering.md).

## Provisioning

With `AutoCreateTopics` on, Twinbox creates missing topics with `TopicPartitions` and `TopicReplicationFactor` before
the first send to them, and at startup for listened topics and the dead-letter topic. Existing topics are left as they
are. With it off, topics must exist (unless the brokers auto-create them); a missing topic dead-letters outbox rows.

## Limitations

- A handler that keeps throwing a transient exception stalls its partition forever. Throw `PermanentDeliveryException`
  for records that can never succeed.
- Without `DeadLetterTopic`, permanently failing records are dropped after logging and counting them.
- One consumer thread per listener handles all its assigned partitions one batch at a time; scale out with more
  instances or partitions.
- Offsets are committed synchronously per record or batch, which trades throughput for simple redelivery bounds.
