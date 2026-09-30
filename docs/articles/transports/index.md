# Transports

A transport sends outbox messages to a broker and, when you register listeners, feeds received messages through the
inbox. Register one or several; with several, name the transport on each route (see [Routing](../concepts/routing.md)).

## At a glance

| Transport | A destination is | Listen with | Batch receive | When a handler fails | Permanent failures go to |
|---|---|---|---|---|---|
| [Azure Service Bus](azure-service-bus.md) | queue or topic | `Listen(queue)`, `Listen(topic, subscription)` | no | abandoned after a backoff (`RetryDelay`) until the entity's max delivery count | the entity's dead-letter subqueue |
| [Azure Event Hubs](event-hubs.md) | event hub | `Listen(hub, consumerGroup, checkpoints)` | yes | retried in place with backoff; the partition waits, no attempt limit | `DeadLetterEventHub`, or logged and skipped |
| [Amazon SQS / SNS](amazon-sqs.md) | queue, or `sns:` topic | `Listen(queue)`, `Listen(queue, topic)` | no | hidden with backoff; the queue's redrive policy sets the limit | `DeadLetterQueue`, the redrive policy, or logged and deleted |
| [Google Pub/Sub](google-pubsub.md) | topic | `Listen(subscription, topic)` | no | nacked; the subscription's retry and dead-letter policy apply | `DeadLetterTopic`, the subscription's policy, or logged and acknowledged |
| [RabbitMQ](rabbitmq.md) | exchange (routing key = message name) | `Listen(queue, exchange, bindingKey)` | no | requeued after a backoff (`RetryDelay`) until `MaxDeliveryAttempts` | `<queue>.dlq` |
| [Kafka](kafka.md) | topic | `Listen(topic, groupId)` | yes | retried in place with backoff; the partition waits, no attempt limit | `DeadLetterTopic`, or logged and skipped |
| [NATS JetStream](nats.md) | subject | `Listen(stream, durableConsumer, filterSubject)` | no | nacked with backoff until `MaxDeliveryAttempts` | `DeadLetterSubject`, or logged and terminated |
| [Redis Streams](redis-streams.md) | stream | `Listen(stream, group)` | no | reclaimed after `ClaimIdleAfter` until `MaxDeliveryAttempts` | `<stream>:dead` |
| [Apache Pulsar](pulsar.md) | topic | `Listen(topic, subscription)` | no | redelivered after a backoff (`RetryDelay`) until `MaxDeliveryAttempts` | `<topic>-dlq` |
| [HTTP](http.md) | configured endpoint | (send only; see [Webhooks](../webhooks.md)) | | | |
| [Local delivery](local.md) | a label | (handlers run in-process) | no | the outbox retries the send | dead in the outbox |

"Batch receive" means [batch handlers](../concepts/batch-handlers.md) can get more than one message per call.

## What every transport does the same

- **Sending** is driven by the outbox dispatcher. A broker error that retrying can't fix (a missing queue, a message
  that's too large) throws `PermanentDeliveryException` and dead-letters the outbox row; anything else is retried with
  the outbox's [backoff and circuit breaker](../concepts/retries-and-dead-letters.md).
- **Sending errors about access** (missing permissions, a revoked key, an ACL or IAM denial, HTTP 401/403) are always
  retried, never dead-lettered: fixing the permission releases the backlog.
- **Receiving** acknowledges a message only after the inbox transaction has committed. A handler exception means
  redelivery; `PermanentDeliveryException` means dead-letter.
- **Retry limits** are called `MaxDeliveryAttempts` wherever Twinbox enforces one, and count every delivery, the first
  included: with 10, the tenth failure dead-letters the message. Where the broker owns the limit (Service Bus's max
  delivery count, an SQS redrive policy), the broker's setting applies instead. Kafka and Event Hubs retry in place
  without a limit.
- **Redelivery backoff** is `RetryDelay`, doubling per attempt up to `MaxRetryDelay`, on every transport that
  redelivers (Redis Streams waits `ClaimIdleAfter`; Pub/Sub follows the subscription's retry policy).
- **Shortcut overloads** (`UseKafka(bootstrapServers)`, `UseRabbitMQ(connectionString)`, ...) take an optional
  callback for the remaining options, listeners included.

## Permanent failures without a dead-letter destination

When a handler throws `PermanentDeliveryException`, a transport with a native dead-letter mechanism (Service Bus's
dead-letter subqueue, RabbitMQ's dead-letter exchange, Redis Streams' and Pulsar's dead streams and topics, an SQS
redrive policy, a Pub/Sub subscription's dead-letter policy) uses it. When there is none and no Twinbox dead-letter
option (`DeadLetterTopic`, `DeadLetterQueue`, `DeadLetterSubject`, `DeadLetterEventHub`) is set, every transport does
the same: it logs the failure at error level with the message id, name and source, adds one to the
`twinbox.inbox.discarded` counter, and acknowledges the message so it doesn't block or loop forever. This applies to
Kafka, Event Hubs, NATS, SQS (queues without a redrive policy) and Pub/Sub (subscriptions without a dead-letter
policy). Alert on that counter, or set a dead-letter option, if such messages must be kept.

Messages a transport does move to a dead-letter destination are counted in `twinbox.inbox.dead_lettered`. Both
counters carry `transport` and `source` tags and come from the `Twinbox` meter (see [Observability](../observability.md)).
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
