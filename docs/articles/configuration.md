# Configuration reference

The core options (`TwinboxOptions`) are bound from the `Twinbox` configuration section. Configuration values override
what you set in code, so operators can tune a deployment without rebuilding it.

```csharp
builder.Services.AddTwinbox(twinbox => twinbox
    .Configure(o =>
    {
        o.Dispatcher.BatchSize = 200;
        o.Retry.MaxAttempts = 15;
    }));
```

```json
{
  "Twinbox": {
    "DestinationPrefix": "staging-",
    "Dispatcher": { "BatchSize": 200, "MaxDegreeOfParallelism": 8 },
    "Retry": { "MaxAttempts": 15, "MaxDelay": "00:10:00" },
    "CircuitBreaker": { "FailureThreshold": 5, "BreakDuration": "00:00:30" },
    "Destinations": {
      "partner-api": { "Retry": { "MaxAttempts": 30 } }
    },
    "Retention": { "SentMessages": "1.00:00:00", "InboxEntries": "7.00:00:00" },
    "Inbox": { "UnknownMessages": "DeadLetter" }
  }
}
```

## Root

| Key | Default | Description |
|---|---|---|
| `InstanceId` | `{machine}-{pid}-{guid}` | Lease owner name. Must be unique per running process; the default already is. |
| `DestinationPrefix` | `null` | Prepended once to every logical destination (routes, `SendOptions.Destination`, webhooks), not to reply addresses. See [Routing](concepts/routing.md#destination-prefix). |

## `Dispatcher`

| Key | Default | Description |
|---|---|---|
| `Enabled` | `true` | Runs the background dispatcher. Turn off to dispatch with `ITwinboxMaintenance` yourself. |
| `BatchSize` | `100` | Rows claimed per pass (per store and tenant). Must be positive. |
| `LeaseDuration` | `00:00:30` | How long claimed rows are held before another instance may take them. |
| `MinPollInterval` | `00:00:01` | Idle delay right after work was found. |
| `MaxPollInterval` | `00:00:30` | Longest idle delay; also the wait after a failed pass. |
| `MaxDegreeOfParallelism` | processor count | Destination groups sent concurrently within a batch. |
| `SendTimeout` | `00:00:30` | Cap on a single send; a timeout counts as a failed attempt. |

Keep `LeaseDuration` longer than a batch takes to send, or another instance may claim the same rows and send them
again.

## `Retry`

| Key | Default | Description |
|---|---|---|
| `MaxAttempts` | `10` | Attempts before a message is dead, the first included. Must be positive. |
| `InitialDelay` | `00:00:01` | Backoff ceiling for the first retry; doubles each attempt. |
| `MaxDelay` | `00:05:00` | Upper bound on the backoff. |
| `MaxRetryAfter` | `01:00:00` | Upper bound on a transport's `Retry-After` hint. |

## `CircuitBreaker`

| Key | Default | Description |
|---|---|---|
| `FailureThreshold` | `5` | Consecutive failures that open a destination's breaker; `0` disables it. |
| `BreakDuration` | `00:00:30` | How long an open breaker holds messages back. |

## `Destinations:{name}`

`Retry` and `CircuitBreaker` blocks that override the global ones for one destination, keyed by its logical name
(without `DestinationPrefix`). See [Retries](concepts/retries-and-dead-letters.md#per-destination-settings).

## `Retention`

| Key | Default | Description |
|---|---|---|
| `Enabled` | `true` | Runs the cleanup service. Turn off to call `ITwinboxMaintenance.RunCleanupAsync` from your own scheduler. |
| `SentMessages` | `1.00:00:00` | Age at which sent rows are deleted. |
| `DeadMessages` | `null` | Age at which dead rows are deleted; `null` keeps them. |
| `InboxEntries` | `7.00:00:00` | Age at which inbox entries are deleted: the deduplication window. |
| `CleanupInterval` | `00:05:00` | Time between cleanup runs. |
| `BatchSize` | `1000` | Rows per delete statement. |

## `Inbox`

| Key | Default | Description |
|---|---|---|
| `Enabled` | `true` | Deduplicate and run handlers in a transaction. |
| `UnknownMessages` | `DeadLetter` | `DeadLetter` or `Ignore` a message with no registered type or handler. |

## Other sections

| Section | Page |
|---|---|
| `Twinbox:Http:Endpoints:{name}` | [HTTP transport](transports/http.md#configuration) |

Store and broker options (connection strings, listeners, broker tuning) are set in code through each `Use...` method.
Read them from configuration yourself if you want them there:

```csharp
twinbox.UseKafka(o =>
{
    o.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"]!;
    o.Listen("orders", groupId: "shipping");
});
```

Invalid values (`BatchSize` or `MaxAttempts` not positive, an empty `InstanceId`) fail options validation when the
options are first used.
