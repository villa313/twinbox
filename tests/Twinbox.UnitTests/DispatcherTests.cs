using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class DispatcherTests
{
    [Fact]
    public async Task TransientFailure_IsRetriedWithBackoff()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));
        var failures = 2;
        host.Harness.Transport.OnSend = _ => failures-- > 0 ? throw new TimeoutException("broker slow") : Task.CompletedTask;

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        var row = Assert.Single(host.Harness.Store.Snapshot());
        Assert.Equal(OutboxMessageStatus.Pending, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.Contains("broker slow", row.LastError, StringComparison.Ordinal);
        Assert.True(row.AvailableAt > host.Time.GetUtcNow());

        host.Time.Advance(TimeSpan.FromMinutes(10));
        await host.Harness.DrainAsync();
        host.Time.Advance(TimeSpan.FromMinutes(10));
        await host.Harness.DrainAsync();

        Assert.Single(host.Harness.Transport.Sent);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Harness.Store.Snapshot()).Status);
    }

    [Fact]
    public async Task PermanentFailure_DeadLettersImmediatelyAndNotifiesObservers()
    {
        var observer = new RecordingObserver();
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders"),
            s => s.AddSingleton<IDeadLetterObserver>(observer));
        host.Harness.Transport.OnSend = _ => throw new PermanentDeliveryException("queue does not exist");

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        var dead = Assert.Single(host.Harness.DeadLettered());
        Assert.Equal(1, dead.Attempts);
        Assert.Equal(dead.Id, Assert.Single(observer.MessageIds));
    }

    [Fact]
    public async Task ExhaustedRetries_DeadLetter()
    {
        await using var host = TestHost.Create(b => b
            .Route<OrderPlaced>().To("orders")
            .Configure(o =>
            {
                o.Retry.MaxAttempts = 3;
                o.CircuitBreaker.FailureThreshold = 0;
            }));
        host.Harness.Transport.OnSend = _ => throw new TimeoutException();

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        for (var i = 0; i < 3; i++)
        {
            await host.Harness.DrainAsync();
            host.Time.Advance(TimeSpan.FromMinutes(10));
        }

        Assert.Equal(3, Assert.Single(host.Harness.DeadLettered()).Attempts);
    }

    [Fact]
    public async Task DestinationOverride_UsesItsOwnRetryPolicy()
    {
        await using var host = TestHost.Create(b => b
            .Route<OrderPlaced>().To("fragile")
            .Configure(o => o.Destinations["fragile"] = new DestinationOptions { Retry = new RetryOptions { MaxAttempts = 1 } }));
        host.Harness.Transport.OnSend = _ => throw new TimeoutException();

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        Assert.Single(host.Harness.DeadLettered());
    }

    [Fact]
    public async Task OpenCircuit_DefersWithoutSpendingAttempts()
    {
        await using var host = TestHost.Create(b => b
            .Route<OrderPlaced>().To("orders")
            .Configure(o => o.CircuitBreaker = new CircuitBreakerOptions { FailureThreshold = 1, BreakDuration = TimeSpan.FromMinutes(1) }));
        host.Harness.Transport.OnSend = _ => throw new TimeoutException();

        await host.SendAsync(o =>
        {
            o.Send(new OrderPlaced(1));
            o.Send(new OrderPlaced(2));
        });
        await host.Harness.DrainAsync();

        var rows = host.Harness.Store.Snapshot();
        Assert.Equal(1, rows[0].Attempts);
        Assert.Equal(0, rows[1].Attempts);
        Assert.Equal(host.Time.GetUtcNow().AddMinutes(1), rows[1].AvailableAt);
    }

    [Fact]
    public async Task UnknownTransport_DeadLetters()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders", transport: "missing"));

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        Assert.Contains("missing", Assert.Single(host.Harness.DeadLettered()).LastError, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FailingObserver_DoesNotBreakDispatch()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders"),
            s => s.AddSingleton<IDeadLetterObserver>(new ThrowingObserver()));
        host.Harness.Transport.OnSend = _ => throw new PermanentDeliveryException("nope");

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        Assert.Single(host.Harness.DeadLettered());
    }

    private sealed class RecordingObserver : IDeadLetterObserver
    {
        public List<Guid> MessageIds { get; } = [];

        public Task OnDeadLetteredAsync(OutboxMessage message, Exception exception, CancellationToken cancellationToken)
        {
            MessageIds.Add(message.Id);
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingObserver : IDeadLetterObserver
    {
        public Task OnDeadLetteredAsync(OutboxMessage message, Exception exception, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("observer bug");
    }
}
