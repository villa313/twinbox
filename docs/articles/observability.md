# Observability

Twinbox emits OpenTelemetry traces and metrics under one name, `TwinboxDiagnostics.SourceName` (`"Twinbox"`), and
provides a health check. With .NET Aspire, `AddTwinboxServiceDefaults()` wires all of it up; see [Aspire](aspire.md).

## OpenTelemetry

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(TwinboxDiagnostics.SourceName))
    .WithMetrics(m => m.AddMeter(TwinboxDiagnostics.SourceName));
```

### Traces

A trace follows a message end to end, across the database commit and the broker:

1. `outbox.Send` captures the current W3C trace context (for example the incoming HTTP request's span) and stores it
   with the outbox row.
2. The dispatcher starts a **producer** span as a child of that context, named `{destination} send`. Its id goes out
   in the `traceparent` header.
3. The receiving side starts a **consumer** span from `traceparent`, named `{source} process`. Handler work, database
   calls and messages sent from the handler nest under it.

| Span | Kind | Tags |
|---|---|---|
| `{destination} send` | Producer | `messaging.system` (transport name), `messaging.destination.name`, `messaging.message.id` |
| `{source} process` | Consumer | `messaging.message.id`, `messaging.source.name` |

A failed send marks the producer span with an error status.

### Metrics

| Instrument | Type | Unit | Tags | Meaning |
|---|---|---|---|---|
| `twinbox.outbox.sent` | Counter | | `destination` | Messages delivered to a transport. |
| `twinbox.outbox.failed` | Counter | | `destination` | Failed delivery attempts (each retry counts). |
| `twinbox.outbox.dead_lettered` | Counter | | `destination` | Outgoing messages that gave up. |
| `twinbox.outbox.delivery_latency` | Histogram | ms | `destination` | Time from `Send` (row creation) to delivery. Includes any `Delay`. |
| `twinbox.inbox.processed` | Counter | | | Incoming messages handled. |
| `twinbox.inbox.duplicates` | Counter | | | Incoming messages skipped as already processed. |
| `twinbox.inbox.dead_lettered` | Counter | | `transport`, `source` | Incoming messages a transport moved to a dead-letter destination. |
| `twinbox.inbox.discarded` | Counter | | `transport`, `source` | Incoming messages that failed permanently and were acknowledged because no dead-letter destination exists. See [Transports](transports/index.md#permanent-failures-without-a-dead-letter-destination). |

Useful alerts: `dead_lettered` above zero, `failed` rate climbing for one destination, and the p99 of
`delivery_latency`. The backlog itself (pending count and age) comes from the health check or the
[dashboard](dashboard.md)'s `api/stats`.

## Health checks

```csharp
builder.Services.AddHealthChecks().AddTwinbox(
    maxPendingAge: TimeSpan.FromMinutes(10),
    maxDeadMessages: 0,
    tags: ["ready"]);

app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = c => c.Tags.Contains("ready") });
```

| Parameter | Default | Meaning |
|---|---|---|
| `name` | `"twinbox"` | Registration name. |
| `maxPendingAge` | 5 minutes | Report `failureStatus` when a pending message has been due for longer than this. |
| `maxDeadMessages` | `0` | Report `deadLetterStatus` when more messages than this are dead. |
| `failureStatus` | `Degraded` | Status for a stale backlog. |
| `deadLetterStatus` | same as `failureStatus` | Status for too many dead messages. |
| `tags` | none | Tags for filtering, e.g. `ready`. |

### One check per concern

A stale backlog usually recovers once a broker comes back; dead letters never do. To alert on them differently,
register one check for each instead of `AddTwinbox`:

```csharp
builder.Services.AddHealthChecks()
    .AddTwinboxBacklog(maxPendingAge: TimeSpan.FromMinutes(10))   // "twinbox-backlog", Degraded
    .AddTwinboxDeadLetters();                                     // "twinbox-dead-letters", Unhealthy
```

`AddTwinboxBacklog` takes `name`, `maxPendingAge`, `failureStatus` (default `Degraded`) and `tags`;
`AddTwinboxDeadLetters` takes `name`, `maxDeadMessages` (default `0`), `failureStatus` (default `Unhealthy`) and
`tags`. Each looks only at its own concern.

The check reads statistics from every store and tenant and reports `pending` and `dead` counts in its data. Pending
age is measured from when a message became due (its `AvailableAt`), so [delayed sends](concepts/delayed-send.md) don't
count as backlog until their time comes. A retried message becomes due again at its next attempt, so a destination that
keeps failing shows up in the dead count and the `twinbox.outbox.failed` metric rather than in pending age.

Keep it out of liveness probes: a broker outage grows the backlog, and restarting the app doesn't fix the broker.

## Logs

Twinbox logs through `Microsoft.Extensions.Logging` with categories named after its types
(`Twinbox.Dispatch.OutboxDispatcher`, `Twinbox.Inbox.InboundPipeline`, and so on). Worth knowing:

| Level | Event |
|---|---|
| Warning | A send failed and will be retried (with the next attempt time). |
| Error | A message was dead-lettered; the dispatcher pass failed; messages were sent but never saved in a scope. |
| Debug | A duplicate was skipped by the inbox. |
| Information | An unknown message was ignored (with `UnknownMessages = Ignore`); dashboard replays and deletes. |

Message payloads and webhook secrets are never logged.
