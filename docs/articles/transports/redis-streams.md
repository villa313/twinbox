# Redis Streams

`Twinbox.RedisStreams` · transport name `"redisstreams"` (`RedisStreamsTransport.TransportName`)

A route destination is a **stream key**. Each message is appended with `XADD`. Listeners read a stream as a member of
a **consumer group**.

## Setup

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseRedisStreams("redis:6379,password=secret")
    .Route<OrderPlaced>().To("orders"));
```

The string is a StackExchange.Redis configuration string. Unless it sets `abortConnect`, Twinbox turns
`AbortOnConnectFail` off, so the app starts while Redis is down and reconnects in the background.

To reuse a multiplexer the app already owns, use the `Action<RedisStreamsOptions>` overload with
`ConnectionFactory`. It takes precedence over `Configuration`, and Twinbox never disposes it:

```csharp
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect("redis:6379"));
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseRedisStreams(redis =>
    {
        redis.ConnectionFactory = sp => sp.GetRequiredService<IConnectionMultiplexer>();
        redis.MaxLength = 100_000;
    })
    .Route<OrderPlaced>().To("orders"));
```

## Receiving

Register a listener per stream with `Listen(stream, group)`:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseRedisStreams(redis =>
    {
        redis.Configuration = "redis:6379";
        redis.Listen("orders", group: "billing");
    })
    .AddHandler<OrderPlacedHandler, OrderPlaced>());
```

Every instance joins the group under `ConsumerName`, which must differ between instances. Instances in one group
share the stream's entries; each group gets every entry.

`MessageContext.Source` is the stream key. Entries are handled one at a time, so batch handlers get batches of one
(see [batch handlers](../concepts/batch-handlers.md)).

## Options

| Option | Default | Description |
|---|---|---|
| `Configuration` | `null` | StackExchange.Redis configuration string. Required unless `ConnectionFactory` is set. |
| `ConnectionFactory` | `null` | `Func<IServiceProvider, IConnectionMultiplexer>` supplying an app-owned multiplexer. Never disposed by Twinbox. |
| `MaxLength` | `null` | Caps each stream sent to at roughly this many entries (`XADD MAXLEN ~`). `null` keeps every entry. Must be positive. |
| `ClaimIdleAfter` | 1 min | Idle time before an unacknowledged entry is reclaimed. Also the retry delay. At least 1 ms. |
| `MaxDeliveries` | `10` | Deliveries of an entry before it moves to the dead stream. Must be positive. |
| `BatchSize` | `10` | Entries read or reclaimed per round trip. Must be positive. |
| `PollInterval` | 500 ms | Wait before reading again after a read found nothing. Must be positive. |
| `ConsumerName` | `"<machine>-<process id>"` | This process's name in each consumer group. Required. |

Options are set in code; they are not bound from configuration.

## Message mapping

Each entry has these fields:

| Field | Content |
|---|---|
| `id` | Message id |
| `name` | Message name |
| `content-type` | Content type |
| `body` | Payload bytes |
| `headers` | JSON object with the other headers, including `twinbox-partition-key` |

On receive, the id comes from the `id` field. An entry without one (written by another producer) gets the stream
coordinates `<stream>:<entry id>`, which stay the same across redeliveries, so the inbox still deduplicates it.
A missing `content-type` defaults to `application/octet-stream`. An entry whose `headers` field is not valid JSON is
dead-lettered. `MessageContext.DeliveryAttempt` is 1 for a new entry, and the server's delivery count for a reclaimed
one.

## Failures, retries and dead letters

**Sending.** `WRONGTYPE` (the key is not a stream) and `NOPERM` (missing ACL grant) are permanent: the outbox row is
dead-lettered. Connection errors, timeouts, OOM and failovers are retried by the outbox. There is no "unroutable"
case: `XADD` creates the stream, and entries nobody reads simply stay there. See
[retries and dead letters](../concepts/retries-and-dead-letters.md).

**Receiving.**

- Handler succeeds: the entry is acknowledged (`XACK`).
- Any other exception: the entry is left pending. A sweep runs every `ClaimIdleAfter / 2` and reclaims entries idle
  for `ClaimIdleAfter` (`XAUTOCLAIM`), so a retry happens between 1x and 1.5x `ClaimIdleAfter` later. The sweep also
  picks up entries left by crashed instances.
- Handler throws `PermanentDeliveryException`, or fails on delivery `MaxDeliveries`: the entry is dead-lettered.
- A reclaimed entry already past `MaxDeliveries` (earlier deliveries never reported back, e.g. the process crashed)
  is dead-lettered without running the handler.

Dead-lettering appends a copy to the stream `<stream>:dead` with all original fields plus `error` (exception type and
message), `origin` (`<stream>:<entry id>`) and `group`, then acknowledges the original. The copy keeps the message id.
All groups of a stream share one dead stream. If the copy fails, the entry stays pending and is tried again.

## Ordering

The partition key is only carried in the `headers` field. Each listener reads its stream in order, one entry at a
time. A failed entry waits for the sweep while later entries go on, so it is overtaken. Several instances in one
group split the entries and do not keep order. See [ordering](../concepts/ordering.md).

## Provisioning

- Sending needs nothing: `XADD` creates the stream.
- Each listener creates its stream and consumer group (`XGROUP CREATE ... 0-0 MKSTREAM`) when missing. A new group
  starts at the beginning of the stream, so it reads entries added before it existed. An existing group is kept.
- The dead stream is created on the first dead letter.

There is no switch to turn this off.

## Limitations

- `ClaimIdleAfter` is both the retry delay and the crash-detection timeout. Keep it above your longest handler run
  time, or another instance reclaims an entry that is still being handled.
- The default `ConsumerName` contains the process id, so each restart joins the group as a new consumer. Entries the
  old consumer left pending wait for the sweep, and old consumers are never removed from the group.
- `MaxLength` trimming can drop entries a group has not read yet. It is not applied to dead streams, which grow
  without bound.
- One entry is handled at a time per listener; there is no concurrency option.
- The destination prefix (`Twinbox:DestinationPrefix`) applies to routed streams, not to `Listen` stream names.
