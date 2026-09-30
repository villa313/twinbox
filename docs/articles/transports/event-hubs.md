# Azure Event Hubs

`Twinbox.EventHubs` · transport name `"eventhubs"` (`EventHubsTransport.TransportName`)

A route destination is the name of an event hub in the configured namespace. Each outbox message becomes one event,
sent with the partition key so events of one key land on one partition. On the receiving side Twinbox runs a partition
processor per listened event hub and consumer group, and checkpoints to Azure Blob Storage only after the inbox has
handled an event.

## Setup

With a connection string, and optionally a callback for everything else, listeners included:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseEventHubs(
        builder.Configuration.GetConnectionString("EventHubs")!,
        options => options.Listen("orders", "billing", storageConnectionString, "checkpoints"))
    .Route<OrderPlaced>().To("orders"));
```

With a token credential:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseEventHubs("my-namespace.servicebus.windows.net", new DefaultAzureCredential())
    .Route<OrderPlaced>().To("orders"));
```

Or set everything on the options:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseEventHubs(options =>
    {
        options.FullyQualifiedNamespace = "my-namespace.servicebus.windows.net";
        options.Credential = new DefaultAzureCredential();
        options.DeadLetterEventHub = "orders-dead-letter";
        options.Listen("orders", "billing", new Uri("https://myaccount.blob.core.windows.net/checkpoints"));
    })
    .Route<OrderPlaced>().To("orders"));
```

Either `ConnectionString`, or `FullyQualifiedNamespace` together with `Credential`, is required.

## Receiving

`Listen` takes the event hub, the consumer group and a blob container for checkpoints, in one of three forms:

```csharp
options.Listen("orders", "billing", new Uri("https://myaccount.blob.core.windows.net/checkpoints"));
options.Listen("orders", "billing", storageConnectionString, "checkpoints");
options.Listen("orders", "billing", new BlobContainerClient(storageConnectionString, "checkpoints"));
```

A container given by URI is reached with `Credential` when it is set; otherwise the URI must carry a SAS token.

The message `Source` is the event hub name. Several app instances with the same consumer group share the partitions
between them. Each owned partition is read in its own loop, so a slow or failing partition does not hold up the others.

## Options

| Option | Default | Description |
|---|---|---|
| `ConnectionString` | `null` | Namespace connection string. Leave unset to use `Credential`. |
| `FullyQualifiedNamespace` | `null` | For example `my-namespace.servicebus.windows.net`. Used with `Credential`. |
| `Credential` | `null` | A `TokenCredential`. Also used for checkpoint containers given by URI. |
| `ConfigureProducer` | `null` | Last say over each producer's `EventHubProducerClientOptions`. |
| `ConfigureProcessor` | `null` | Last say over each listener's `EventProcessorOptions`, for example the default starting position. |
| `CreateCheckpointContainers` | `false` | Create missing checkpoint containers when a listener starts. Needs permission to create containers. |
| `DeadLetterEventHub` | `null` | Event hub that receives copies of permanently failing events. `null` logs and skips them. Cannot be blank. |
| `RetryDelay` | 1 second | First delay before a failed event is retried. Doubles per attempt. Must be positive. |
| `MaxRetryDelay` | 30 seconds | Cap on the retry delay. Cannot be shorter than `RetryDelay`. |
| `MaxBatchSize` | `1` | Most events of one partition handed to the pipeline at once. Must be positive. |

When `MaxBatchSize` is above the processor's prefetch count, the prefetch count is raised to match before
`ConfigureProcessor` runs.

## Message mapping

| Twinbox | Event Hubs |
|---|---|
| Message id | `MessageId`, and property `twinbox-message-id` |
| Message name | property `twinbox-message-name` |
| Content type | `ContentType` |
| Partition key | send option `PartitionKey` |
| Headers | application properties |

On receive, the message id is the `twinbox-message-id` property, then `MessageId`, then the event's coordinates
`<event hub>:<partition id>:<sequence number>`. Coordinates are stable across redeliveries, so events from other
producers are still deduplicated. The partition key is the event's `PartitionKey`, falling back to the
`twinbox-partition-key` property. A missing content type becomes `application/octet-stream`. Events without a
`twinbox-message-name` property need a [header profile](../concepts/headers.md) that supplies the name.

## Failures, retries and dead letters

Sending: an `EventHubsException` with reason `ResourceNotFound` or `MessageSizeExceeded` is permanent and dead-letters
the outbox row. An event too large for one batch is caught before it is sent and counts as `MessageSizeExceeded`.
Access errors stay transient, so fixing a role assignment releases the backlog. See
[Retries and dead letters](../concepts/retries-and-dead-letters.md).

Receiving: Event Hubs has no acknowledgement or redelivery, so Twinbox retries in place.

- Handler succeeds: the partition is checkpointed past the event.
- Any other exception: the event is retried after `RetryDelay`, doubling up to `MaxRetryDelay`. There is no attempt
  limit. Only that partition waits.
- `PermanentDeliveryException`: the event is copied to `DeadLetterEventHub` with properties `twinbox-error` and
  `twinbox-origin` (`<event hub>:<partition>:<sequence number>`), then checkpointed past. Without a dead-letter hub
  it is logged at error level, counted in `twinbox.inbox.discarded` and checkpointed past (see
  [the transports overview](index.md#permanent-failures-without-a-dead-letter-destination)). If the copy fails, the
  event is retried like a handler failure.

The delivery attempt is counted in memory and starts again at 1 after a restart or a partition moving to another
instance. A failed checkpoint write only means the event may be read again, which the inbox deduplicates.

With `MaxBatchSize` above 1, a failed batch is retried event by event so the failing event can be isolated. See
[Batch handlers](../concepts/batch-handlers.md).

## Ordering

The partition key selects the partition, so events with one key are stored in order. Each partition is processed
sequentially and a failing event blocks the events behind it until it succeeds or is dead-lettered, so per-key order
holds on the receiving side too. Events without a partition key are spread across partitions and have no order. See
[Ordering](../concepts/ordering.md).

## Provisioning

Event hubs and consumer groups are never created; they must exist, including `DeadLetterEventHub`. Checkpoint
containers are created only when `CreateCheckpointContainers` is on. If a listener cannot start (missing hub, container
or permission), it retries with backoff up to 30 seconds until it succeeds or the host stops.

## Limitations

- A handler that keeps throwing a transient exception stalls its partition forever. Throw `PermanentDeliveryException`
  for messages that can never succeed.
- Without `DeadLetterEventHub`, permanently failing events are dropped after logging and counting them.
- One consumer group per `Listen` call; the same event hub under two consumer groups reports the same `Source`.
