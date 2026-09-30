# Apache Pulsar

`Twinbox.Pulsar` · transport name `"pulsar"` (`PulsarTransport.TransportName`)

A route destination is a **topic**. One producer per topic sends each message, keyed by its partition key. Listeners
read a topic through a **subscription**.

## Setup

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UsePulsar("pulsar://pulsar:6650")
    .Route<OrderPlaced>().To("persistent://public/default/orders"));
```

The string must be an absolute `pulsar://` or `pulsar+ssl://` address; an optional callback after it, like the
`Action<PulsarOptions>` overload, exposes every option. `ConfigureClient` gets the `IPulsarClientBuilder` last, for authentication, TLS or a listener name:

```csharp
twinbox.UsePulsar(pulsar =>
{
    pulsar.ServiceUrl = new Uri("pulsar+ssl://broker.example.com:6651");
    pulsar.ConfigureClient = client => client.Authentication(AuthenticationFactory.Token(builder.Configuration["Pulsar:Token"]!));
    pulsar.SubscriptionType = SubscriptionType.KeyShared;
});
```

## Receiving

Register a listener per subscription with `Listen(topic, subscription)`:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UsePulsar(pulsar =>
    {
        pulsar.ServiceUrl = new Uri("pulsar://pulsar:6650");
        pulsar.ConsumerConcurrency = 4;
        pulsar.Listen("persistent://public/default/orders", subscription: "billing");
    })
    .AddHandler<OrderPlacedHandler, OrderPlaced>());
```

Each listener runs `ConsumerConcurrency` consumers on the subscription; each consumer handles one message at a time.
The subscription is created when missing, starting at `InitialPosition`.

`MessageContext.Source` is the topic as passed to `Listen`. Messages are handled one at a time, so batch handlers get
batches of one (see [batch handlers](../concepts/batch-handlers.md)).

## Options

| Option | Default | Description |
|---|---|---|
| `ServiceUrl` | `null` | Broker address, `pulsar://` or `pulsar+ssl://`. Required. |
| `ConfigureClient` | `null` | `Action<IPulsarClientBuilder>` with the last say over the client. |
| `SubscriptionType` | `Shared` | Subscription type used by listeners. |
| `InitialPosition` | `Earliest` | Where a subscription created by a listener starts reading. |
| `MaxDeliveryAttempts` | `10` | Deliveries of a message, the first included; a failure on the last one moves it to the dead-letter topic. Must be positive. |
| `RetryDelay` | 1 s | Wait before a failed message is handed back to the broker; doubles per attempt. Must be positive. |
| `MaxRetryDelay` | 30 s | Cap on `RetryDelay` doubling, at most about 24 days. |
| `DeadLetterSuffix` | `"-dlq"` | Appended to a listener's topic to name its dead-letter topic. Required. |
| `ConsumerConcurrency` | `1` | Consumers per listener. Above 1 needs a `Shared` or `KeyShared` subscription. |
| `SendTimeout` | 30 s | Time a send may wait for the broker's confirmation, reconnects included. Must be positive. |

Options are set in code; they are not bound from configuration.

## Message mapping

| Twinbox | Pulsar |
|---|---|
| Destination | Topic |
| Message id | `twinbox-message-id` property |
| Message name | `twinbox-message-name` property |
| Content type | `content-type` property |
| Partition key | Message key |
| Headers | Message properties |

The producer's sequence id is left to the client. Twinbox ids are not monotonic, and broker deduplication would drop
retried messages if they were reused as sequence ids.

On receive, the id comes from the `twinbox-message-id` property. A message without one (from another producer) gets
the coordinates `<topic>:<ledger>:<entry>:<partition>:<batch index>`, which stay the same across redeliveries, so the
inbox still deduplicates it. The content type defaults to `application/octet-stream`. The partition key is the
message key. `MessageContext.DeliveryAttempt` is the broker's redelivery count plus one.

## Failures, retries and dead letters

**Sending.** Topic not found, invalid topic name, message too large, terminated topic, incompatible schema and "not
allowed" errors are permanent: the outbox row is dead-lettered. Authorization failures, a timeout (`SendTimeout`) or a
faulted producer are retried by the outbox; a faulted producer is replaced. See
[retries and dead letters](../concepts/retries-and-dead-letters.md).

**Receiving.**

- Handler succeeds: the message is acknowledged.
- Any other exception: the message stays unacknowledged for `RetryDelay` (doubling per attempt up to `MaxRetryDelay`),
  then is handed back to the broker for redelivery. The consumer moves on to other messages meanwhile.
- Handler throws `PermanentDeliveryException`, or fails on attempt `MaxDeliveryAttempts` or later: the message is
  dead-lettered.

Dead-lettering sends a copy to `<topic><DeadLetterSuffix>` (default `<topic>-dlq`) with the original key, payload and
properties plus `twinbox-error` (exception type and message), `twinbox-origin` (the coordinates) and
`twinbox-subscription`. The copy keeps the message id. Then the original is acknowledged. All subscriptions of a topic
share one dead-letter topic. If the copy fails, the original is redelivered and dead-lettered again later.

The broker counts redeliveries only on `Shared` and `KeyShared` subscriptions. On `Exclusive` and `Failover`, Twinbox
tallies failures per consumer instead, and the tally restarts when the consumer is recreated.

## Ordering

The partition key becomes the message key, so partitioned topics route by it.

- `Shared` (default): messages are spread across consumers with no ordering.
- `KeyShared`: each key goes to one consumer and stays in order, but a failed message is redelivered after later
  messages of its key.
- `Exclusive` and `Failover`: one active consumer. A retry redelivers every unacknowledged message.

See [ordering](../concepts/ordering.md).

## Provisioning

Twinbox creates no topics. Producers and consumers rely on the broker's topic auto-creation; if it is off, the
destination topics and the `-dlq` topics must exist beforehand. Listeners create their subscriptions when missing.

## Limitations

- Retries are not negative acks (DotPulsar has none): a failed message stays with its consumer until its `RetryDelay`
  passes. If the consumer closes meanwhile, the broker redelivers it at once.
- No producer options (batching, compression, chunking) are exposed.
- `ConsumerConcurrency` above 1 is rejected for `Exclusive` and `Failover` subscriptions.
- The destination prefix (`Twinbox:DestinationPrefix`) applies to routed topics, not to `Listen` topic names.
