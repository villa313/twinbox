# RabbitMQ

`Twinbox.RabbitMQ` · transport name `"rabbitmq"` (`RabbitMqTransport.DefaultName`)

A route destination is a **topic exchange**. Each message is published to that exchange with its message name as the
routing key. Listeners consume **quorum queues** bound to an exchange with a binding key.

## Setup

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseRabbitMq(rabbit =>
    {
        rabbit.HostName = "rabbitmq";
        rabbit.UserName = "app";
        rabbit.Password = builder.Configuration["RabbitMq:Password"]!;
    })
    .Route<OrderPlaced>().To("orders"));
```

`UseRabbitMq` has a single overload that takes an `Action<RabbitMqOptions>`. To connect with a URI, set
`ConnectionUri`; it overrides the host, port, virtual host and credentials:

```csharp
twinbox.UseRabbitMq(rabbit => rabbit.ConnectionUri = new Uri("amqps://app:secret@broker.example.com/prod"));
```

For TLS details or credential providers, `ConfigureConnectionFactory` gets the `ConnectionFactory` last:

```csharp
twinbox.UseRabbitMq(rabbit =>
{
    rabbit.HostName = "broker.example.com";
    rabbit.ConfigureConnectionFactory = factory => factory.Ssl.Enabled = true;
});
```

The connection is opened on first use and recovered automatically. If the broker is down at startup, listeners keep
retrying (1 s doubling to 30 s) instead of failing the host.

## Receiving

Register a listener per queue with `Listen(queue, exchange, bindingKey = "#")`:

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseRabbitMq(rabbit => rabbit
        .Listen("billing.orders", exchange: "orders")
        .Listen("billing.refunds", exchange: "payments", bindingKey: "RefundIssued"))
    .AddHandler<OrderPlacedHandler, OrderPlaced>());
```

The default binding key `#` receives every message published to the exchange. Since the routing key is the message
name, a binding key equal to a message name receives only that message type.

`MessageContext.Source` is the queue name. Each delivery goes through the inbox pipeline on its own, so batch
handlers get batches of one (see [batch handlers](../concepts/batch-handlers.md)).

## Options

| Option | Default | Description |
|---|---|---|
| `ConnectionUri` | `null` | `amqp://` or `amqps://` URI. Overrides host, port, virtual host and credentials. |
| `HostName` | `"localhost"` | Broker host. Required unless `ConnectionUri` is set. |
| `Port` | `null` | `null` uses the protocol default (5672, or 5671 for TLS). |
| `VirtualHost` | `"/"` | Virtual host. |
| `UserName` | `"guest"` | User name. |
| `Password` | `"guest"` | Password. |
| `ClientProvidedName` | `"twinbox"` | Connection name shown in the management UI. |
| `ConfigureConnectionFactory` | `null` | Last say over the `ConnectionFactory`, e.g. TLS or credential providers. |
| `AutoProvision` | `true` | Declare exchanges, queues, bindings and dead-letter topology on first use. |
| `DeadLetterUnroutable` | `false` | Dead-letter unroutable publishes at once instead of retrying them. |
| `DeliveryLimit` | `10` | `x-delivery-limit` of declared queues. The broker dead-letters a message once it has been returned more than this many times. Must be positive. |
| `PrefetchCount` | `20` | Unacknowledged deliveries per listener channel. Must be positive. |
| `ConsumerConcurrency` | `1` | Deliveries processed in parallel per listener. Above 1, ordering within a queue is lost. |
| `PublishChannelPoolSize` | `Environment.ProcessorCount` | Publisher-confirm channels kept for sending; each carries one publish at a time. Must be positive. |
| `PublishTimeout` | 30 s | Time to get a channel and a publisher confirm. Must be positive. |

Options are set in code; they are not bound from configuration.

## Message mapping

| Twinbox | RabbitMQ |
|---|---|
| Destination | Exchange name |
| Message name | Routing key and `type` property |
| Message id | `message_id` property |
| Content type | `content_type` property |
| Partition key | `twinbox-partition-key` header |
| Headers | AMQP headers |

Messages are published as persistent with `mandatory` set.

On receive, the id comes from `message_id`, then the `twinbox-message-id` header. The name comes from `type`, then
the `twinbox-message-name` header. A message with neither id is rejected as permanently failed, unless a
[header profile](../concepts/headers.md) supplies one. The content type defaults to `application/octet-stream`.
`MessageContext.DeliveryAttempt` is `x-delivery-count + 1` on quorum queues; elsewhere it is 2 when the redelivered
flag is set, else 1. Headers that are tables or arrays (such as `x-death`) are not passed on.

## Failures, retries and dead letters

**Sending.** Each publish waits for a publisher confirm.

- Exchange not found (404) or access refused (403): permanent, the outbox row is dead-lettered.
- Unroutable (the exchange returned the message because no queue is bound): retried by the outbox, since a consumer
  may not have bound its queue yet. Set `DeadLetterUnroutable = true` to dead-letter it at once.
- No confirm or no free channel within `PublishTimeout`, connection errors: retried by the outbox.

See [retries and dead letters](../concepts/retries-and-dead-letters.md) for the outbox side.

**Receiving.**

- Handler succeeds: the delivery is acked.
- Handler throws `PermanentDeliveryException`: the delivery is nacked without requeue, so the broker dead-letters it.
- Any other exception: the delivery is nacked with requeue and redelivered immediately. There is no delay between
  attempts. Once a message has been returned more than `DeliveryLimit` times, the quorum queue dead-letters it.

Dead-lettered messages go through the direct exchange `twinbox.dead-letter` to the queue `<queue>.dlq`.

## Ordering

The partition key is only carried as a header; RabbitMQ does not route by it. Order within a queue holds only with
`ConsumerConcurrency = 1` and a single consuming instance. A failed delivery is requeued, so messages already
prefetched can overtake it. Several instances consuming the same queue share its messages and do not keep order.
See [ordering](../concepts/ordering.md).

## Provisioning

With `AutoProvision = true` (the default):

- The transport declares each destination exchange as a durable topic exchange before its first publish. Built-in
  `amq.*` exchanges are not declared.
- Each listener declares its exchange (topic, durable), the direct exchange `twinbox.dead-letter`, the quorum queue
  `<queue>.dlq` bound to it with routing key `<queue>.dlq`, and the queue itself, bound with the binding key.

Listener queues are declared with these arguments:

| Argument | Value |
|---|---|
| `x-queue-type` | `quorum` |
| `x-delivery-limit` | `DeliveryLimit` |
| `x-dead-letter-exchange` | `twinbox.dead-letter` |
| `x-dead-letter-routing-key` | `<queue>.dlq` |

With `AutoProvision = false`, nothing is declared. Exchanges, queues and bindings must exist, and queues need their
own dead-letter configuration, or rejected messages are dropped by the broker.

## Limitations

- No connection-string overload of `UseRabbitMq`; use `ConnectionUri`.
- Failed deliveries are requeued immediately, with no backoff between attempts.
- Queues are always quorum queues. If a queue already exists with different arguments (for example another
  `DeliveryLimit`), the broker refuses the declaration and the listener keeps retrying with a warning.
- Quorum queues dead-letter at most once by default; a dead-lettered message can be lost if `<queue>.dlq` rejects it.
- The destination prefix (`Twinbox:DestinationPrefix`) applies to routed exchanges, not to `Listen` queue or exchange
  names.
- Messages without an AMQP `message_id` or `twinbox-message-id` header cannot be deduplicated and are dead-lettered.
