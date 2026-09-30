# Testing

`Twinbox.Testing` works with any test framework. It gives you a deterministic harness for testing your own messaging
code, and conformance suites for testing a custom store.

## The test harness

`UseTestHarness()` swaps in the in-memory store and transport and turns off every background loop. You drive
dispatch and delivery yourself, so tests don't wait on timers.

```csharp
[Fact]
public async Task Placing_an_order_ships_it()
{
    var services = new ServiceCollection()
        .AddLogging()
        .AddTwinbox(twinbox => twinbox
            .UseTestHarness()
            .Route<OrderPlaced>().To("orders")
            .Route<ShipmentRequested>().To("shipping")
            .AddHandler<ShipOrder, OrderPlaced>());
    await using var provider = services.BuildServiceProvider();

    await using (var scope = provider.CreateAsyncScope())
    {
        scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(Guid.NewGuid()));
        await scope.ServiceProvider.GetRequiredService<InMemoryUnitOfWork>().CommitAsync();
    }

    var harness = provider.GetTwinboxHarness();
    await harness.DrainAsync();   // dispatches and delivers until nothing is left, including follow-up messages

    Assert.Single(harness.Sent<ShipmentRequested>());
    Assert.Empty(harness.DeadLettered());
}
```

`DrainAsync` alternates dispatch and delivery until a round does nothing. Messages that handlers send are dispatched in
the next round. If handlers keep sending forever, it throws after 1,000 rounds.

| Member | Use |
|---|---|
| `DrainAsync()` | Dispatch and deliver until settled. |
| `Sent<T>()` | Every message of type `T` that reached the transport, deserialized. |
| `DeadLettered()` | Outbox rows that ended up `Dead` (sending gave up). |
| `Store` | The `InMemoryOutboxStore`; `Store.Snapshot()` returns every row. |
| `Transport` | The `InMemoryTransport`: `Sent` (raw messages with headers), `DeadLettered` (deliveries that failed permanently), `OnSend`. |

### Simulating failures

```csharp
var harness = provider.GetTwinboxHarness();
harness.Transport.OnSend = _ => throw new TimeoutException("broker down");
```

Retries are scheduled in the future, so pair failure tests with a fake clock. Register a
`FakeTimeProvider` (from `Microsoft.Extensions.TimeProvider.Testing`) before `AddTwinbox`, advance it past the backoff,
and drain again:

```csharp
var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
services.AddSingleton<TimeProvider>(time);
// ...
await harness.DrainAsync();
time.Advance(TimeSpan.FromMinutes(10));
harness.Transport.OnSend = null;
await harness.DrainAsync();
```

### Testing a handler directly

The pipeline is public, so you can feed a message to your handlers the way a transport would:

```csharp
var pipeline = provider.GetRequiredService<IInboundPipeline>();
await pipeline.ProcessAsync(new IncomingMessage(
    MessageId: "m-1",
    MessageName: "OrderPlaced",
    Source: "orders",
    Body: Encoding.UTF8.GetBytes("""{"orderId":"4b8e2c1e-7f5d-4c3a-9d2e-1a6f0b9c8d7e"}"""),
    ContentType: "application/json",
    Headers: new Dictionary<string, string>(),
    DeliveryAttempt: 1,
    PartitionKey: null), CancellationToken.None);
```

Process the same message twice to check that your handler runs once.

## Integration tests with a real database

The harness replaces the store. To test against your real schema, keep your store registration and use
`UseInMemoryTransport(o => o.AutoDeliver = false)` with `Dispatcher:Enabled = false`, then call
`IOutboxDispatcher.DispatchBatchAsync` and `InMemoryTransport.DeliverAsync(pipeline, ct)` yourself, the same loop
`DrainAsync` runs.

## Store conformance

If you write your own `IOutboxStore`, run the conformance suites against it. They are plain lists of named cases, so
they plug into any framework's data-driven tests:

```csharp
public sealed class MyStoreConformanceTests
{
    public static TheoryData<ConformanceCase<IOutboxStore>> Cases =>
        [.. OutboxStoreConformance.Cases, .. OutboxAdminConformance.Cases];

    [Theory]
    [MemberData(nameof(Cases))]
    public Task Store_meets_the_contract(ConformanceCase<IOutboxStore> conformanceCase) =>
        conformanceCase.RunAsync(CreateEmptyStore());
}
```

`OutboxStoreConformance` covers appending, claiming, leases, partition ordering, completion and purging.
`OutboxAdminConformance` covers `IOutboxAdmin` (querying, paging, replay, delete); include it only if your store
implements that interface. Give each case a fresh, empty store. A failing case throws `ConformanceException`.
