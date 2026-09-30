# NATS JetStream

`Twinbox.Nats` · transport name `"nats"` (`NatsTransport.TransportName`)

A route destination is a **subject**. Messages are published through JetStream, so a stream must capture that
subject. Listeners read a stream through a **durable pull consumer**, optionally filtered to a subject.

## Setup

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseNats("nats://nats:4222")
    .Route<OrderPlaced>().To("orders.placed"));
```

The `Action<NatsOptions>` overload exposes every option. `ConfigureConnection` gets the final say over the client's
`NatsOpts`, for credentials, NKeys, JWTs or TLS:

```csharp
twinbox.UseNats(nats =>
{
    nats.Url = "nats://nats-1:4222,nats://nats-2:4222";
    nats.ConfigureConnection = opts => opts with { AuthOpts = NatsAuthOpts.Default with { CredsFile = "/secrets/app.creds" } };
    nats.AutoCreateStreams = true;
    nats.AddStream("ORDERS", "orders.>");
});
```

## Receiving

Register a listener per durable consumer with `Listen(stream, durableConsumer, filterSubject = null)`:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseNats(nats => nats
        .Listen("ORDERS", durableConsumer: "billing", filterSubject: "orders.placed")
        .Listen("PAYMENTS", durableConsumer: "billing"))
    .AddHandler<OrderPlacedHandler, OrderPlaced>());
```

The filter subject may use the `*` and `>` wildcards. Without it the consumer reads the whole stream. Instances that
use the same durable consumer name share its messages.

`MessageContext.Source` is the subject the message was published to. Messages are handed to the pipeline one at a
time, so batch handlers get batches of one (see [batch handlers](../concepts/batch-handlers.md)).

## Options

| Option | Default | Description |
|---|---|---|
| `Url` | `"nats://localhost:4222"` | Server URL, or a comma-separated list. Required. |
| `ClientName` | `"twinbox"` | Connection name reported to the server. |
| `ConfigureConnection` | `null` | `Func<NatsOpts, NatsOpts>` with the last say over the connection options. |
| `AutoCreateStreams` | `false` | Create the streams registered with `AddStream` before first use. |
| `DuplicateWindow` | 2 min | Duplicate window of streams created here. Must be positive. |
| `DeadLetterUnroutable` | `false` | Dead-letter sends to a subject no stream captures at once instead of retrying them. |
| `DeadLetterSubject` | `null` | Subject that permanently failed messages are copied to. `null` only logs and terminates them. Cannot be blank. |
| `AckWait` | 30 s | Time the server waits for an ack before redelivering. Must be positive. |
| `MaxDeliver` | `10` | Deliveries per message; a failure on the last one dead-letters it. Must be positive. |
| `MaxAckPending` | `1000` | Unacknowledged messages outstanding per consumer, across all instances. Must be positive. |
| `PrefetchCount` | `20` | Messages each listener pulls ahead. `AckWait` runs for them while they wait. Must be positive. |
| `ConsumerConcurrency` | `1` | Messages processed in parallel per listener. Above 1, ordering is lost. Must be positive. |
| `RetryDelay` | 1 s | First redelivery delay after a handler fails; doubles per attempt. Must be positive. |
| `MaxRetryDelay` | 30 s | Cap on the redelivery delay. Cannot be shorter than `RetryDelay`. |

`AddStream(name, params subjects)` registers a stream for `AutoCreateStreams`. Options are set in code; they are not
bound from configuration.

## Message mapping

| Twinbox | NATS |
|---|---|
| Destination | Subject |
| Message id | `Nats-Msg-Id` and `twinbox-message-id` headers |
| Message name | `twinbox-message-name` header |
| Content type | `content-type` header |
| Partition key | `twinbox-partition-key` header |
| Headers | NATS headers |

`Nats-Msg-Id` lets JetStream drop a repeated send within the stream's duplicate window, so an outbox retry after a
lost ack does not store the message twice.

On receive, the id comes from `twinbox-message-id`, then `Nats-Msg-Id`, then the stream coordinates
`<stream>:<sequence>`. Coordinates stay the same across redeliveries, so the inbox still deduplicates messages from
other publishers. The content type defaults to `application/octet-stream`. `MessageContext.DeliveryAttempt` is the
server's delivery count.

## Failures, retries and dead letters

**Sending.** Each publish waits for the JetStream ack.

- JetStream API errors 400, 403 or 413, "message too large", permission violations and payloads over the server's
  maximum: permanent, the outbox row is dead-lettered.
- No stream captures the subject: retried by the outbox, since the stream may not exist yet. Set
  `DeadLetterUnroutable = true` to dead-letter it at once.
- Other rejections (for example a full stream or no JetStream response) and connection errors: retried.

A duplicate ack counts as success. See [retries and dead letters](../concepts/retries-and-dead-letters.md).

**Receiving.**

- Handler succeeds: the message is acked.
- Any other exception: the message is nacked with a delay of `RetryDelay * 2^(attempt - 1)`, capped at `MaxRetryDelay`.
- Handler throws `PermanentDeliveryException`, or fails on delivery `MaxDeliver`: the message is dead-lettered.
- Host shutting down: in-flight messages are nacked without delay so another instance can take them.

Dead-lettering copies the message to `DeadLetterSubject` with its headers plus `twinbox-error` (exception type and
message), `twinbox-origin` (`<stream>:<sequence>`) and `Nats-Msg-Id: dead-letter:<origin>`, then terminates the
original. If the copy fails, the original is nacked and tried again. With no `DeadLetterSubject`, the message is only
logged and terminated.

## Ordering

The partition key is only carried as a header; JetStream does not route by it. With `ConsumerConcurrency = 1`, one
instance handles a consumer's messages in stream order. A failed message is nacked with a delay, so later messages
overtake it. Instances sharing a durable consumer split its messages and do not keep order. See
[ordering](../concepts/ordering.md).

## Provisioning

- Listeners always create or update their durable consumer with explicit acks, `AckWait`, `MaxDeliver`,
  `MaxAckPending` and the filter subject. Settings changed on the server are overwritten at startup.
- Streams are created only when `AutoCreateStreams = true`, and only those registered with `AddStream`. They get
  `DuplicateWindow`; all other settings are server defaults. An existing stream is kept as is.
- Otherwise every stream must exist beforehand, including one that captures `DeadLetterSubject`.

## Limitations

- No in-progress acks: a handler running longer than `AckWait` gets its message redelivered while it still runs.
  Prefetched messages also use up `AckWait` while they wait.
- If copying to `DeadLetterSubject` fails on the last allowed delivery, the server will not deliver the message
  again; it stays unacknowledged in the stream.
- `DuplicateWindow` applies only to streams created by Twinbox.
- The destination prefix (`Twinbox:DestinationPrefix`) applies to routed subjects, not to `Listen` or `AddStream`
  names.
