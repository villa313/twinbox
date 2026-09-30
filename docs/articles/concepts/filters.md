# Filters

## Incoming: `IMessageFilter`

An `IMessageFilter` wraps every handler call, like middleware. It runs inside the handler's DI scope and inside its
inbox transaction, so a filter that throws rolls the handler back just like the handler throwing would.

```csharp
public class LogHandling(ILogger<LogHandling> logger) : IMessageFilter
{
    public async Task InvokeAsync(object message, MessageContext context, Func<Task> continuation, CancellationToken ct)
    {
        using var _ = logger.BeginScope(new Dictionary<string, object?>
        {
            ["MessageId"] = context.MessageId,
            ["CorrelationId"] = context.CorrelationId,
        });

        var started = Stopwatch.GetTimestamp();
        await continuation();
        logger.LogInformation("Handled {MessageName} in {Elapsed}", context.MessageName, Stopwatch.GetElapsedTime(started));
    }
}

twinbox.AddFilter<LogHandling>();
```

- Filters are scoped services and run in registration order; the first registered is the outermost.
- They run once per handler, not once per message: a message with two handlers passes through the filters twice.
- Skipping `continuation()` skips the handler, and the message still counts as handled (the inbox entry commits).
  Throw `PermanentDeliveryException` instead if the message should be dead-lettered.
- Duplicates never reach filters: the inbox check happens first.

## Outgoing: `IOutgoingMessageFilter`

An `IOutgoingMessageFilter` runs synchronously inside `outbox.Send`, once per message, before routing. Use it to stamp
headers from ambient context:

```csharp
public class StampTenantRegion(RegionContext region) : IOutgoingMessageFilter
{
    public void OnSending(object message, IDictionary<string, string> headers) =>
        headers["x-region"] = region.Name;
}

twinbox.AddOutgoingFilter<StampTenantRegion>();
```

Outgoing filters are resolved from the scope that owns the `IOutbox`, so they can use scoped services such as the
current request's user. The headers are saved with the outbox row.
