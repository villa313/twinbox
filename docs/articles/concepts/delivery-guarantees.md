# Delivery guarantees

Twinbox delivers **at least once** and processes **effectively once**. This page spells out what that means and where
the edges are.

## What is guaranteed

- **No lost messages.** A message is written in the same transaction as your data. If the transaction commits, the
  message exists; if it rolls back, it never does. The dispatcher keeps retrying until the message is sent or
  dead-lettered.
- **No phantom messages.** A message from a rolled-back transaction is never sent.
- **At-least-once sending.** A message can be sent more than once: a crash after the broker accepted it but before the
  outbox row was marked sent, an expired lease, or a timeout on a send that actually landed. Every copy carries the
  same message id.
- **Effectively-once handling.** The inbox records (message id, consumer) in the same transaction as the handler's
  writes and outgoing messages. A duplicate is skipped. Handler writes, the inbox entry and new messages commit or roll
  back together.
- **Per-key ordering** for messages with a partition key, up to the broker. See [Ordering](ordering.md).

## Where the guarantee stops

- **Side effects outside the transaction.** An email sent or an HTTP API called directly from a handler isn't rolled
  back and can happen twice. Send a message instead (for example through the [HTTP transport](../transports/http.md),
  which adds an idempotency key), or make the call idempotent.
- **The deduplication window.** Inbox entries are purged after `Retention:InboxEntries` (7 days). A redelivery after
  that runs the handler again.
- **Handlers without an inbox.** With `Inbox:Enabled = false`, or with no store registered, handlers run at least
  once.
- **Retrying execution strategies.** With EF Core's retry-on-failure, a transient database error can run the handler
  body again after a rollback. Database effects stay exactly-once; external effects don't.
- **MongoDB transient transaction errors.** The MongoDB inbox retries a transaction that fails with a transient error
  (for up to two minutes), which reruns the handler.
- **Receivers that aren't Twinbox.** Other consumers see at-least-once delivery. They should deduplicate on the
  message id: the `twinbox-message-id` header, or the broker's own message id property where it has one (Service Bus,
  RabbitMQ), which Twinbox sets to the same value.

## Brokers with their own deduplication

Some transports pass the message id to the broker's native deduplication as well: the NATS `Nats-Msg-Id` header,
the SQS and SNS FIFO deduplication id, and the `Idempotency-Key` header of the HTTP transport. That narrows duplicates
further but doesn't replace the inbox.
