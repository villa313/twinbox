# Twinbox

**Transactional outbox & inbox for .NET.** The one you add, not the framework you adopt.

Twinbox writes your messages in the same transaction as your data and delivers them to any broker,
with built-in deduplication on the receiving side. It works with EF Core, plain ADO.NET and Dapper,
and needs no base classes, no bus abstraction and no special transaction API.

> **Status:** early development. Not ready for production use.

## Goals

- Three-line setup on an existing app
- Outbox **and** inbox: at-least-once delivery, effectively-once processing
- Any broker: Azure Service Bus, RabbitMQ, Kafka, Amazon SQS/SNS, NATS, Redis Streams, Pulsar, HTTP, and more
- Any store: SQL Server, PostgreSQL, MySQL, SQLite, Oracle, MongoDB
- Webhook ingress with signature verification
- OpenTelemetry, health checks, dashboard, Aspire, Native AOT
- A test harness that makes messaging code easy to test

## License

[MIT](LICENSE), forever.
