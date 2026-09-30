# Transports

A transport sends outbox messages to a broker and, when you register listeners, feeds received messages through the
inbox. Register one or several; with several, name the transport on each route (see [Routing](../concepts/routing.md)).

## At a glance

| Transport | A destination is | Listen with | Batch receive | When a handler fails | Permanent failures go to |
|---|---|---|---|---|---|
| [Azure Service Bus](azure-service-bus.md) | queue or topic | `Listen(queue)`, `Listen(topic, subscription)` | no | abandoned; redelivered at once until the entity's max delivery count | the entity's dead-letter subqueue |
| [Azure Event Hubs](event-hubs.md) | event hub | `Listen(hub, consumerGroup, checkpoints)` | yes | retried in place with backoff; the partition waits, no attempt limit | `DeadLetterEventHub`, or logged and skipped |
| [Amazon SQS / SNS](amazon-sqs.md) | queue, or `sns:` topic | `ListenToQueue(queue)`, `Subscribe(queue, topic)` | no | hidden with backoff; the queue's redrive policy sets the limit | `DeadLetterQueue`, or the redrive policy |
| [Google Pub/Sub](google-pubsub.md) | topic | `Subscribe(subscription, topic)` | no | nacked; the subscription's retry and dead-letter policy apply | `DeadLetterTopic`, or the subscription's policy |
| [RabbitMQ](rabbitmq.md) | exchange (routing key = message name) | `Listen(queue, exchange, bindingKey)` | no | requeued at once until `DeliveryLimit` | `<queue>.dlq` |
| [Kafka](kafka.md) | topic | `Listen(topic, groupId)` | yes | retried in place with backoff; the partition waits, no attempt limit | `DeadLetterTopic`, or logged and skipped |
| [NATS JetStream](nats.md) | subject | `Listen(stream, durableConsumer, filterSubject)` | no | nacked with backoff until `MaxDeliver` | `DeadLetterSubject`, or terminated |
| [Redis Streams](redis-streams.md) | stream | `Listen(stream, group)` | no | reclaimed after `ClaimIdleAfter` until `MaxDeliveries` | `<stream>:dead` |
| [Apache Pulsar](pulsar.md) | topic | `Listen(topic, subscription)` | no | redelivered after `NegativeAckRedeliveryDelay` until `MaxRedeliveryCount` | `<topic>-dlq` |
| [HTTP](http.md) | configured endpoint | (send only; see [Webhooks](../webhooks.md)) | | | |
| [Local delivery](local.md) | a label | (handlers run in-process) | no | the outbox retries the send | dead in the outbox |

"Batch receive" means [batch handlers](../concepts/batch-handlers.md) can get more than one message per call.

## What every transport does the same

- **Sending** is driven by the outbox dispatcher. A broker error that retrying can't fix (a missing queue, a message
  that's too large) throws `PermanentDeliveryException` and dead-letters the outbox row; anything else is retried with
  the outbox's [backoff and circuit breaker](../concepts/retries-and-dead-letters.md).
- **Receiving** acknowledges a message only after the inbox transaction has committed. A handler exception means
  redelivery; `PermanentDeliveryException` means dead-letter.
- **Identity**: the message id travels with the message (as a broker property or a `twinbox-message-id` header). For
  messages from other producers without one, most transports fall back to something stable across redeliveries (the
  broker's message id or the record's coordinates), so deduplication still works.
- **Tracing**: `traceparent` is sent and picked up on the other side. See [Observability](../observability.md).
- **Startup**: listeners keep retrying with backoff when the broker is unreachable, rather than failing the host.

## Choosing destination names

Destinations are plain strings from your routes, plus the optional
[destination prefix](../concepts/routing.md#destination-prefix). Listener names (queues, subscriptions, groups) are
configured separately and are not prefixed.

## Writing a transport

Implement `Twinbox.Transport.ITransport` (`Name` and `SendAsync`) and register it as a singleton `ITransport`. Throw
`PermanentDeliveryException` for failures that retrying can't fix, and `RetryAfterException` when the broker says how
long to wait. For receiving, call `IInboundPipeline.ProcessAsync` (or `ProcessBatchAsync`) for each message and
settle it according to the outcome: returned means acknowledge, `PermanentDeliveryException` means dead-letter,
anything else means redeliver.
