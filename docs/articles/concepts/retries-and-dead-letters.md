# Retries, circuit breaker and dead letters

This page covers the sending side: what the dispatcher does when a transport fails. The receiving side (a handler
throws) is up to the broker and the transport; see [The inbox](inbox.md) and each transport's page.

## Retries

A failed send is rescheduled with exponential backoff and jitter:

- The ceiling for attempt *n* is `InitialDelay * 2^(n-1)`, capped at `MaxDelay`.
- The actual delay is picked at random between half the ceiling and the ceiling, so a burst of failures doesn't retry
  in lockstep.
- After `MaxAttempts` attempts in total (the first send included), the message is dead.

| Option | Default |
|---|---|
| `Twinbox:Retry:MaxAttempts` | `10` |
| `Twinbox:Retry:InitialDelay` | `00:00:01` |
| `Twinbox:Retry:MaxDelay` | `00:05:00` |
| `Twinbox:Retry:MaxRetryAfter` | `01:00:00` |

A transport can ask for a specific wait by throwing `RetryAfterException` (the HTTP transport does this for
`Retry-After`). The dispatcher waits at least that long, capped at `MaxRetryAfter`.

Each attempt's error (type and message, up to 2,000 characters) is stored on the row as `LastError`, visible in the
[dashboard](../dashboard.md).

## Permanent failures

Some errors can't be fixed by waiting: a queue that doesn't exist, a message that's too large, an HTTP 400.
Transports signal these with `PermanentDeliveryException`, and the message is dead-lettered on the first attempt. A
route to a transport name that isn't registered is also dead-lettered right away.

## Circuit breaker

Each (transport, destination) pair has a circuit breaker. After `FailureThreshold` consecutive failures it opens for
`BreakDuration`: messages for that destination are put back without being sent and without using up an attempt. After
the break, one failure reopens it at once; one success closes it.

| Option | Default |
|---|---|
| `Twinbox:CircuitBreaker:FailureThreshold` | `5` (`0` turns the breaker off) |
| `Twinbox:CircuitBreaker:BreakDuration` | `00:00:30` |

Breaker state is kept in memory per instance.

## Per-destination settings

Override retry and breaker settings for one destination. The key is the logical destination, without the
[destination prefix](routing.md#destination-prefix), so the same configuration works in every environment. Keys are
case-insensitive. A destination's settings apply on every transport that sends to it; each transport keeps its own
breaker state.

```json
{
  "Twinbox": {
    "Retry": { "MaxAttempts": 10 },
    "Destinations": {
      "partner-webhook": {
        "Retry": { "MaxAttempts": 20, "MaxDelay": "01:00:00" },
        "CircuitBreaker": { "FailureThreshold": 3, "BreakDuration": "00:02:00" }
      }
    }
  }
}
```

The same in code:

```csharp
twinbox.Configure(o => o.Destinations["partner-webhook"] = new DestinationOptions
{
    Retry = new RetryOptions { MaxAttempts = 20, MaxDelay = TimeSpan.FromHours(1) },
});
```

Configuration values override code, so operators can tune a destination without a redeploy.

## Dead letters

A dead message stays in the outbox table with status `Dead` and its last error. Nothing more happens to it until you:

- replay it (back to `Pending` with a fresh attempt count) or delete it from the [dashboard](../dashboard.md) or its
  JSON API, or through `IOutboxAdmin`;
- let [retention](retention.md) purge it, if `Retention:DeadMessages` is set (off by default).

To react as it happens, implement `IDeadLetterObserver`:

```csharp
public class AlertOnDeadLetter(ILogger<AlertOnDeadLetter> logger) : IDeadLetterObserver
{
    public Task OnDeadLetteredAsync(OutboxMessage message, Exception exception, CancellationToken ct)
    {
        logger.LogCritical(exception, "Gave up on {MessageName} {Id} to {Destination}",
            message.MessageName, message.Id, message.Destination);
        return Task.CompletedTask;
    }
}

builder.Services.AddSingleton<IDeadLetterObserver, AlertOnDeadLetter>();
```

Observers are singletons called from the dispatcher; a failing observer is logged and doesn't affect the message. The
`twinbox.outbox.dead_lettered` metric and the [health check](../observability.md#health-checks) cover the same ground
without code.
