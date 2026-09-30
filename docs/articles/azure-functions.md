# Azure Functions

Functions apps (isolated worker) can be scaled in or stopped at any moment, so background services are not a
reliable place for the outbox dispatcher. `Twinbox.AzureFunctions` turns Twinbox's background work off and lets your
own functions do it: a timer dispatches the outbox, a Service Bus trigger feeds the inbox, and another timer runs
cleanup.

## Setup

```csharp
var builder = FunctionsApplication.CreateBuilder(args);

builder.Services.AddDbContext<AppDbContext>(o => o.UseSqlServer(connectionString));
builder.Services.AddTwinbox(twinbox => twinbox
    .UseEntityFrameworkCore<AppDbContext>()
    .UseAzureServiceBus(serviceBusConnectionString)   // sending; no Listen(): triggers receive
    .UseAzureFunctions()
    .Route<OrderShipped>().To("shipments")
    .AddHandler<ShipOrderHandler, OrderPlaced>());

builder.Build().Run();
```

`UseAzureFunctions()` sets `Dispatcher:Enabled` and `Retention:Enabled` to `false` and registers
`TwinboxServiceBusTrigger` and `ITwinboxMaintenance`.

## Functions

```csharp
public sealed class TwinboxFunctions(
    IOutboxDispatcher dispatcher,
    TwinboxServiceBusTrigger twinbox,
    ITwinboxMaintenance maintenance)
{
    [Function("twinbox-dispatch")]
    public Task Dispatch([TimerTrigger("*/10 * * * * *")] TimerInfo timer, CancellationToken ct) =>
        dispatcher.DispatchPendingAsync(ct);

    [Function("orders")]
    public Task Receive(
        [ServiceBusTrigger("orders", Connection = "ServiceBus", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions,
        CancellationToken ct) =>
        twinbox.ProcessServiceBusMessageAsync(message, messageActions, source: "orders", ct);

    [Function("twinbox-cleanup")]
    public Task Cleanup([TimerTrigger("0 0 * * * *")] TimerInfo timer, CancellationToken ct) =>
        maintenance.RunCleanupAsync(ct);
}
```

### Dispatching

`DispatchPendingAsync` claims and sends batches until the outbox is empty or its time budget runs out: 50 seconds by
default (`OutboxDispatcherExtensions.DefaultDispatchBudget`), leaving headroom under the Consumption plan's shortest
timeout. Pass a `TimeSpan` to change it; a batch already started is always finished:

```csharp
await dispatcher.DispatchPendingAsync(TimeSpan.FromMinutes(4), ct);
```

It returns how many messages were claimed. A message sent from an HTTP function waits for the next timer tick, so pick
the schedule for the latency you need. `IOutboxDispatcher.DispatchBatchAsync` runs a single batch if you want finer
control.

### Receiving from Service Bus

`ProcessServiceBusMessageAsync` runs the message through the inbox and settles it itself, so the trigger must set
`AutoCompleteMessages = false`:

| Outcome | Settlement |
|---|---|
| Handled (or a duplicate) | Completed |
| `PermanentDeliveryException` (bad data, unknown message) | Dead-lettered with reason `PermanentDeliveryFailure` |
| Any other exception | Abandoned, so Service Bus redelivers it until the entity's max delivery count |

The function invocation itself succeeds unless settling fails. The `source` argument becomes `MessageContext.Source`;
without it, handlers see `"azureservicebus"` (`TwinboxServiceBusTrigger.DefaultSource`).

### Cleanup

`RunCleanupAsync` applies the [retention](concepts/retention.md) settings for every tenant, even though the retention
service is off, and returns the number of rows removed. Hourly is plenty for most apps.

## Always-ready instances

On plans with always-ready or pre-warmed instances (Premium, Flex Consumption with always-ready, Dedicated) the
background services are safe again. Turn them back on from configuration:

```json
{
  "Twinbox": {
    "Dispatcher": { "Enabled": true },
    "Retention": { "Enabled": true }
  }
}
```

The background dispatcher then sends right after each commit, and the timer function becomes a safety net.

## Other brokers

The dispatch and cleanup functions work with any transport. For receiving, `TwinboxServiceBusTrigger` covers Service
Bus. For other triggers, build an `IncomingMessage` from the trigger's payload and call `IInboundPipeline.ProcessAsync`;
throw from the function to let the trigger's retry policy redeliver.
