# Azure Service Bus

`Twinbox.AzureServiceBus` · transport name `"azureservicebus"` (`AzureServiceBusTransport.TransportName`)

A route destination is the name of a queue or topic. Twinbox sends to it through a `ServiceBusSender` it creates on
first use. On the receiving side it runs one processor per queue or topic subscription you listen to, and settles each
message (complete, abandon or dead-letter) only after the inbox has handled it.

## Setup

With a connection string:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseAzureServiceBus(builder.Configuration.GetConnectionString("ServiceBus")!)
    .Route<OrderPlaced>().To("orders"));
```

With a token credential, or any client you build yourself:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseAzureServiceBus(
        _ => new ServiceBusClient("my-namespace.servicebus.windows.net", new DefaultAzureCredential()),
        options => options.Listen("billing"))
    .Route<OrderPlaced>().To("orders"));
```

Twinbox owns the `ServiceBusClient` either overload creates, and disposes it on shutdown.

## Receiving

Register listeners on the options:

```csharp
.UseAzureServiceBus(connectionString, options => options
    .Listen("billing")                         // a queue
    .Listen("orders", "billing-subscription")  // a topic subscription
    .Listen("payments", sessions: true))       // a session-enabled queue
```

Pass `sessions: true` for session-enabled queues and subscriptions; they are received with a session processor, the
others with a plain one, so one registration can mix both.

The message `Source` your handlers see is the entity path: `billing` for a queue, `orders/Subscriptions/billing-subscription`
for a subscription.

To receive in Azure Functions instead of a hosted processor, use the Service Bus trigger helper described in
[Azure Functions](../azure-functions.md) (`TwinboxServiceBusTrigger.ProcessServiceBusMessageAsync`). Other custom hosts
can call `AzureServiceBusInbound.ToIncomingMessage(message, source)` and pass the result to `IInboundPipeline`; dead-letter
with `AzureServiceBusInbound.PermanentFailureReason` to match the built-in processor.

## Options

| Option | Default | Description |
|---|---|---|
| `SendSessionIds` | `false` | Sets `SessionId` on every sent message: the partition key, or the message id when there is none. Needed to send to session-enabled entities. |
| `MaxConcurrentCalls` | `1` | Messages handled at once per listener. For session listeners, the number of sessions handled at once (one message per session at a time). Must be positive. |
| `PrefetchCount` | `0` | Passed to the processor. Cannot be negative. |
| `RetryDelay` | 1 s | Wait before a failed message is abandoned; doubles per delivery. Must be positive. |
| `MaxRetryDelay` | 30 s | Cap on `RetryDelay` doubling, at most 5 minutes (the processor's lock renewal limit). |
| `Listen(queueName, sessions = false)` | | Consume a queue, with a session processor when `sessions` is true. |
| `Listen(topicName, subscriptionName, sessions = false)` | | Consume a topic subscription, likewise. |

## Message mapping

| Twinbox | Service Bus |
|---|---|
| Message id | `MessageId` |
| Message name | `Subject` |
| Content type | `ContentType` |
| Partition key | application property `twinbox-partition-key`, and `SessionId` when `SendSessionIds` is on |
| Headers | application properties |

On receive, the name is taken from `Subject`, falling back to the `twinbox-message-name` property. The partition key
comes from the `twinbox-partition-key` property, falling back to `SessionId` unless it equals the message id (the
fallback session of an unkeyed message). The delivery attempt is the broker's
`DeliveryCount`. A missing content type becomes `application/octet-stream`. Service Bus always assigns a `MessageId`, so
messages from other senders are deduplicated by that id. Non-string property values are converted to invariant-culture
strings. See [Headers](../concepts/headers.md) for header profiles.

## Failures, retries and dead letters

Sending: these errors are permanent and dead-letter the outbox row at once:

- `ServiceBusException` with reason `MessagingEntityNotFound` (the queue or topic does not exist)
- `ServiceBusException` with reason `MessageSizeExceeded`

Everything else is retried by the outbox with backoff, including `UnauthorizedAccessException`: fixing a role
assignment or key releases the backlog instead of finding it dead-lettered. See [Retries and dead letters](../concepts/retries-and-dead-letters.md).

Receiving:

- Handler succeeds: the message is completed.
- Handler throws `PermanentDeliveryException`: the message is moved to the entity's dead-letter subqueue with reason
  `PermanentDeliveryFailure` and the exception message as the description.
- Any other exception: the message is held for `RetryDelay` (doubling per delivery up to `MaxRetryDelay`) while the
  processor keeps renewing its lock, then abandoned. Service Bus makes it available again and increments
  `DeliveryCount`. Once the entity's `MaxDeliveryCount` is reached, Service Bus dead-letters it itself. A held
  message occupies one of `MaxConcurrentCalls`; when the host is stopping it is abandoned at once.

Settlement uses its own cancellation token, so a message handled during shutdown is still settled rather than left
locked.

## Ordering

Without sessions, Service Bus does not order by key. With `MaxConcurrentCalls = 1` (the default) each listener handles
one message at a time in the order received, but an abandoned message can be overtaken by a later one.

With `SendSessionIds = true` the partition key becomes the `SessionId`, and a `sessions: true` listener receives it. Service Bus hands a session to one receiver at a
time and Twinbox handles one message per session at a time, so messages with the same key are processed in order.
`MaxConcurrentCalls` then sets how many sessions run in parallel. See [Ordering](../concepts/ordering.md).

## Provisioning

Nothing is created. Queues, topics and subscriptions must exist before the app sends or listens. Listen to a
session-enabled entity with `sessions: true`, and send to one with `SendSessionIds` on.

## Limitations

- The retry delay is spent holding the message locked, not scheduling it; a restart meanwhile redelivers it once the
  lock expires. The retry limit is the entity's `MaxDeliveryCount`.
- `SendSessionIds` applies to every send. Entities without sessions ignore the `SessionId`, but partitioned ones use it
  to pick a partition.
- Batch handlers (`IHandleBatch<T>`) receive batches of one. See [Batch handlers](../concepts/batch-handlers.md).
