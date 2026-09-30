# Google Cloud Pub/Sub

`Twinbox.GooglePubSub` · transport name `"googlepubsub"`

A route destination is a Pub/Sub topic: a topic id in the configured project, or a full `projects/<project>/topics/<topic>`
name. On the receiving side Twinbox runs a streaming subscriber per subscription and acknowledges each message only after
the inbox has handled it.

## Setup

With a project id (send only; credentials come from Application Default Credentials):

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseGooglePubSub("my-project")
    .Route<OrderPlaced>().To("orders"));
```

With options, an explicit credential and a subscription:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseGooglePubSub(options =>
    {
        options.ProjectId = "my-project";
        options.Credential = GoogleCredential.FromFile("service-account.json");
        options.DeadLetterTopic = "orders-dead-letter";
        options.Subscribe("billing-orders", "orders");
    })
    .Route<OrderPlaced>().To("orders"));
```

For the emulator, set `EmulatorHost = "localhost:8085"`. When `EmulatorHost` is null, the `PUBSUB_EMULATOR_HOST`
environment variable is still honoured.

## Receiving

`Subscribe(subscription, topic)` consumes a subscription. The topic is only used when `AutoCreate` creates the
subscription. Both may be short ids or full names.

The message `Source` is the subscription exactly as you passed it. `MaxOutstandingMessages` bounds how many handlers run
at once per subscription. If a subscriber fails, it is rebuilt with backoff up to 30 seconds.

## Options

| Option | Default | Description |
|---|---|---|
| `ProjectId` | `""` | Project for short topic and subscription ids. Required. |
| `EmulatorHost` | `null` | `host:port` of the emulator, reached without credentials. Cannot be blank. |
| `Credential` | `null` | A `GoogleCredential`. `null` uses Application Default Credentials. |
| `ConfigurePublisher` | `null` | Last say over each `PublisherClientBuilder`. |
| `ConfigureSubscriber` | `null` | Last say over each `SubscriberClientBuilder`. |
| `AutoCreate` | `false` | Create missing topics and subscriptions before first use. Existing ones are left unchanged. |
| `AckDeadline` | 60 seconds | Lease on a received message; the subscriber extends it while a handler runs. 10 to 600 seconds. |
| `MaxOutstandingMessages` | `100` | Messages leased but not yet acknowledged per subscription. Must be positive. |
| `EnableMessageOrdering` | `false` | Publish with the partition key as ordering key. Auto-created subscriptions enable ordering to match. |
| `DeadLetterTopic` | `null` | Topic that receives copies of permanently failing messages. Cannot be blank. |
| `MaxDeliveryAttempts` | `5` | Deliveries before an auto-created subscription forwards a message to `DeadLetterTopic`. 5 to 100. |

## Message mapping

| Twinbox | Pub/Sub |
|---|---|
| Message id | attribute `twinbox-message-id` |
| Message name | attribute `twinbox-message-name` |
| Content type | attribute `twinbox-content-type` |
| Partition key | attribute `twinbox-partition-key`, and `OrderingKey` when `EnableMessageOrdering` is on |
| Headers | attributes |

On receive, a message without `twinbox-message-id` falls back to the Pub/Sub message id, which stays the same across
redeliveries. The partition key is the `OrderingKey`, falling back to the attribute. The delivery attempt is Pub/Sub's
delivery attempt, which Pub/Sub only reports when the subscription has a dead-letter policy; otherwise it is always 1.
A missing content type becomes `application/octet-stream`. See [Headers](../concepts/headers.md).

## Failures, retries and dead letters

Sending: `NotFound` and `InvalidArgument` are permanent and dead-letter the outbox row, as does a destination that is
not a valid topic name. `ResourceExhausted` with a retry hint is retried after that hint (capped by
`Twinbox:Retry:MaxRetryAfter`). Permission errors stay transient, so fixing IAM releases the backlog. See
[Retries and dead letters](../concepts/retries-and-dead-letters.md).

Receiving:

- Handler succeeds: the message is acknowledged.
- Any other exception: the message is nacked and Pub/Sub redelivers it according to the subscription's retry policy.
  Twinbox does not set a retry policy, so by default redelivery is immediate.
- `PermanentDeliveryException` with `DeadLetterTopic` set: the message is published there, keeping its id and adding
  `twinbox-error` and `twinbox-origin` (the subscription), then acknowledged. If the publish fails, it is nacked.
- `PermanentDeliveryException` without `DeadLetterTopic`: the message is nacked and left to the subscription's own
  dead-letter policy, if any.

Subscriptions created by `AutoCreate` with a `DeadLetterTopic` also get a dead-letter policy, so Pub/Sub forwards any
message after `MaxDeliveryAttempts` deliveries. Pub/Sub drops messages published to a topic without subscriptions, so
subscribe something to the dead-letter topic.

## Ordering

Order is only kept with `EnableMessageOrdering = true`. The partition key then becomes the ordering key, the publisher
enables ordering and auto-created subscriptions enable it too. An existing subscription must have been created with
ordering enabled. After a failed publish, Twinbox resumes the ordering key so the outbox can retry.

When a message of an ordering key is nacked, later messages of that key are nacked too until it comes back, because
Pub/Sub may redeliver them interleaved. A message that never returns (for example one forwarded to a dead-letter topic)
stops holding the key back after 600 seconds plus `AckDeadline`. See [Ordering](../concepts/ordering.md).

## Provisioning

With `AutoCreate` on, Twinbox creates missing topics on first publish, and at startup creates each subscription with
its topic, the dead-letter topic, `AckDeadline`, ordering and dead-letter policy. Existing topics and subscriptions are
never changed. A subscription deleted while running is recreated. With `AutoCreate` off, everything must exist; a
missing topic dead-letters outbox rows because `NotFound` is permanent.

## Limitations

- No batch consumption: batch handlers receive batches of one. See [Batch handlers](../concepts/batch-handlers.md).
- No backoff on nack unless you configure a retry policy on the subscription.
- Without a dead-letter topic or subscription dead-letter policy, a failing message is redelivered forever.
- Subscription settings (`AckDeadline`, ordering, `MaxDeliveryAttempts`) only apply when Twinbox creates the
  subscription.
