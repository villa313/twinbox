# Amazon SQS and SNS

`Twinbox.AmazonSqs` · transport name `"amazonsqs"`

A route destination is an SQS queue name or URL. A destination that starts with `sns:` (`AmazonSqsTransport.TopicPrefix`)
is an SNS topic name or ARN instead, so one message can fan out to several queues. On the receiving side Twinbox
long-polls SQS queues and deletes each message only after the inbox has handled it.

## Setup

With a region (send only; credentials come from the SDK's default chain):

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseAmazonSqs("eu-west-1")
    .Route<OrderPlaced>().To("sns:orders")
    .Route<InvoiceRequested>().To("billing-requests"));
```

With options, explicit credentials and listeners:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseAmazonSqs(options =>
    {
        options.Region = "eu-west-1";
        options.Credentials = new BasicAWSCredentials(accessKey, secretKey);
        options.DeadLetterQueue = "billing-dead-letter";
        options.Subscribe("billing-orders", "orders");
        options.ListenToQueue("billing-requests");
    })
    .Route<OrderPlaced>().To("sns:orders"));
```

For LocalStack, set `ServiceUrl = new Uri("http://localhost:4566")`. When both are set, `Region` becomes the
authentication region.

## Receiving

- `ListenToQueue(queue)` consumes a queue, by name or URL.
- `Subscribe(queue, topic)` consumes a queue fed by an SNS topic. With `AutoCreate` on, Twinbox creates what is missing,
  allows the topic to send to the queue and subscribes it with raw message delivery. Without it, the subscription must
  already exist. The topic may be given with or without the `sns:` prefix, or as an ARN.

The message `Source` is the queue exactly as you passed it (name or URL). Several registrations of the same queue share
one poller.

## Options

| Option | Default | Description |
|---|---|---|
| `Region` | `null` | System name such as `eu-west-1`. `null` lets the SDK resolve it. Cannot be blank. |
| `ServiceUrl` | `null` | Overrides the endpoint, for example LocalStack. |
| `Credentials` | `null` | `AWSCredentials`. `null` uses the SDK's default credential chain. |
| `ConfigureSqs` | `null` | Last say over the `AmazonSQSConfig`. |
| `ConfigureSns` | `null` | Last say over the `AmazonSimpleNotificationServiceConfig`. |
| `AutoCreate` | `false` | Create missing queues, topics and subscriptions before first use. Names ending in `.fifo` become FIFO. |
| `VisibilityTimeout` | 30 seconds | How long a received message stays hidden while it is handled. 1 second to 12 hours. |
| `WaitTimeSeconds` | `20` | Long-poll duration per receive, 0 to 20. |
| `MaxNumberOfMessages` | `10` | Messages fetched per receive, 1 to 10. |
| `MaxConcurrency` | `10` | Messages handled at once per queue. Must be positive. |
| `DeadLetterQueue` | `null` | Queue that receives copies of permanently failing messages. `null` leaves them to the queue's redrive policy. Cannot be blank. |
| `RetryDelay` | 1 second | First delay before a failed message becomes visible again. Doubles per attempt. Must be positive. |
| `MaxRetryDelay` | 5 minutes | Cap on the retry delay. Between `RetryDelay` and 12 hours. |

## Message mapping

Twinbox values travel as SQS or SNS string message attributes:

| Twinbox | Attribute |
|---|---|
| Message id | `twinbox-message-id` |
| Message name | `twinbox-message-name` |
| Content type | `content-type` |
| Partition key | `twinbox-partition-key`, plus `MessageGroupId` on FIFO queues and topics |
| Headers | one attribute each |

SQS allows 10 message attributes. When the headers do not fit, or a header name is not a valid attribute name, the
remaining headers are packed as JSON into a single `twinbox-headers` attribute. `traceparent` and `twinbox-tenant` are
given their own attribute first. Bodies with a textual content type (`text/*`, JSON, XML) are sent as text; other
bodies are base64-encoded and marked with `twinbox-body-encoding: base64`.

On receive, a message without `twinbox-message-id` falls back to the SQS `MessageId`. If the body is an SNS
notification envelope (a subscription without raw delivery), Twinbox unwraps it and uses the SNS message id and
attributes. The delivery attempt is SQS's `ApproximateReceiveCount`. The partition key falls back to the FIFO
`MessageGroupId`. See [Headers](../concepts/headers.md).

## Failures, retries and dead letters

Sending: a missing queue or topic, invalid parameters, invalid message contents and messages that are too long are
permanent and dead-letter the outbox row. Without `AutoCreate`, an SNS topic that cannot be found by name is also
permanent. Access errors stay transient, so fixing an IAM policy releases the backlog. See
[Retries and dead letters](../concepts/retries-and-dead-letters.md).

Receiving:

- Handler succeeds: the message is deleted.
- Any other exception: the message's visibility is set to `RetryDelay` doubled per receive (capped at
  `MaxRetryDelay`), so it comes back after the backoff. Twinbox sets no attempt limit; configure a redrive policy
  (`maxReceiveCount`) on the queue to stop retries.
- `PermanentDeliveryException` with `DeadLetterQueue` set: the message is copied there, keeping its id and adding
  `twinbox-error` and `twinbox-origin` (the source queue), then deleted. If the copy fails, it is retried.
- `PermanentDeliveryException` without `DeadLetterQueue`: the message is left in place and comes back after the
  visibility timeout, until the queue's redrive policy moves it.

A handler interrupted by shutdown hands its message back at once, so another instance can pick it up.

## Ordering

On standard queues the partition key is only an attribute; SQS does not order messages and `MaxConcurrency` handlers
run in parallel.

On FIFO queues and topics (names ending in `.fifo`) the partition key becomes the `MessageGroupId` and the message id
the `MessageDeduplicationId`. Messages without a partition key share the group `twinbox`. Keys or ids SQS would reject
(too long, spaces, non-ASCII) are replaced by a SHA-256 hash. On receive, messages of one group are handled one after
another in receive order, and when one fails the rest of its group is held back with it. See
[Ordering](../concepts/ordering.md).

## Provisioning

With `AutoCreate` on, Twinbox creates missing queues (with `VisibilityTimeout`, FIFO for `.fifo` names), topics,
queue policies and raw-delivery subscriptions, plus the `DeadLetterQueue`. It does not set redrive policies. With
`AutoCreate` off, everything must exist, and topics given by name are resolved with `ListTopics`, which needs that
permission. Pass ARNs and queue URLs to skip lookups.

## Limitations

- No batch consumption: batch handlers receive batches of one. See [Batch handlers](../concepts/batch-handlers.md).
- Without a redrive policy on the queue and without `DeadLetterQueue`, a failing message is retried forever.
- The two overloads differ: `UseAmazonSqs(region)` cannot register listeners.
- `DeadLetterQueue` must be an SQS queue, not an SNS topic.
