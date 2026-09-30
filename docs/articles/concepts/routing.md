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

`SendOptions.Destination` bypasses the type's routes, for example to answer on a reply queue:

```csharp
outbox.Send(new PriceQuote(42m), new SendOptions { Destination = "quotes-replies", Transport = "rabbitmq" });
```

## Destination prefix

`Twinbox:DestinationPrefix` is prepended to every routed destination, which keeps environments that share a broker
apart:

```json
{ "Twinbox": { "DestinationPrefix": "staging-" } }
```

`Route<OrderPlaced>().To("orders")` then sends to `staging-orders`. Listeners are not renamed: configure them with the
prefixed names. The prefix is applied when the message is saved, so changing it doesn't move messages already in the
outbox.

The prefix is **not** applied to `SendOptions.Destination` (and therefore not to `outbox.Reply`), since those name an
address that already exists. The HTTP transport strips the prefix again when it looks up an endpoint, so endpoints
keep their plain names.
