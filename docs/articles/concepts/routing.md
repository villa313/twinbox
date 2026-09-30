# Routing

A route maps a message type to a destination and, optionally, a transport:

```csharp
twinbox
    .Route<OrderPlaced>().To("orders")
    .Route<OrderPlaced>().To("analytics")                        // route again to fan out
    .Route<InvoiceIssued>().To("billing", transport: "kafka");
```

What a destination means is up to the transport: a Service Bus queue or topic, a RabbitMQ exchange, a Kafka topic, an
HTTP endpoint name, and so on. Each route produces its own outbox row, so each destination is retried and
dead-lettered independently.

Sending a type without a route throws `InvalidOperationException` from `Send`.

## Choosing a transport

With one transport registered, routes use it. With several, every route must name one, or `Send` throws. Transport
names are constants on each transport class:

| Transport | Name |
|---|---|
| Azure Service Bus | `"azureservicebus"` |
| Azure Event Hubs | `"eventhubs"` |
| Amazon SQS / SNS | `"amazonsqs"` |
| Google Pub/Sub | `"googlepubsub"` |
| RabbitMQ | `"rabbitmq"` |
| Kafka | `"kafka"` |
| NATS JetStream | `"nats"` |
| Redis Streams | `"redisstreams"` |
| Pulsar | `"pulsar"` |
| HTTP | `"http"` |
| Local delivery | `"local"` |
| In-memory | `"inmemory"` |

## Base types and interfaces

A route for a base class or interface covers its subtypes. The exact type's routes win; then the nearest base class
with routes; then an implemented interface with routes. Routing and serialization use the runtime type, so events
collected as a base type still reach their own routes:

```csharp
twinbox.Route<IDomainEvent>().To("domain-events", transport: "local");

IDomainEvent e = new OrderPlaced(id);
outbox.Send(e);   // routed and serialized as OrderPlaced
```

## Sending to an explicit address

`SendOptions.Destination` bypasses the type's routes:

```csharp
outbox.Send(new PriceQuote(42m), new SendOptions { Destination = "quotes", Transport = "rabbitmq" });
```

To answer a request, use `outbox.Reply(context, response)`, which sends to the request's reply address (see
[Correlation and replies](correlation-and-reply.md)).

## Destination prefix

`Twinbox:DestinationPrefix` keeps environments that share a broker apart:

```json
{ "Twinbox": { "DestinationPrefix": "staging-" } }
```

Twinbox tells apart **logical** destinations, the names you write in code and configuration, and **physical** ones, the
addresses the broker sees. One rule connects them:

- **The prefix is applied exactly once, when a logical destination is resolved.** That covers routes
  (`Route<OrderPlaced>().To("orders")` sends to `staging-orders`), `SendOptions.Destination`, and the destinations of
  [webhook](../webhooks.md) endpoints.
- **Reply addresses are physical.** `SendOptions.ReplyTo` is sent as given and `outbox.Reply` sends to it as given, so
  set it to the address your listener really uses. `options.ToPhysicalDestination("checkout-replies")` (on
  `TwinboxOptions`) builds it with the prefix when that listener follows the prefix.
- **Configuration is keyed by the logical name.** `Twinbox:Destinations:orders` applies to `staging-orders`. Those
  settings apply to that destination on every transport; each transport still gets its own circuit breaker.
- **HTTP endpoint names are logical.** The HTTP transport looks endpoints up by the unprefixed name, so endpoints keep
  their plain names.
- **Listeners are named by you.** Twinbox doesn't rename queues, subscriptions or topics you listen on; configure them
  with the physical names. Transports that want listener names to follow the prefix use
  `TwinboxOptions.ToPhysicalDestination`, and `ToLogicalDestination` goes the other way.

The prefix is applied when the message is saved, so changing it doesn't move messages already in the outbox.
