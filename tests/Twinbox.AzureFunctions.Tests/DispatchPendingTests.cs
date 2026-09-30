using Microsoft.Extensions.Time.Testing;

namespace Twinbox.AzureFunctions.Tests;

public sealed class DispatchPendingTests
{
    [Fact]
    public async Task DrainsBatchesUntilTheOutboxIsEmpty()
    {
        var time = new FakeTimeProvider();
        int[] batches = [100, 100, 30];
        var dispatcher = new ScriptedDispatcher(time, TimeSpan.FromSeconds(1), call => call < batches.Length ? batches[call] : 0);

        var claimed = await dispatcher.DispatchPendingAsync(TimeSpan.FromSeconds(50), time, default);

        Assert.Equal(230, claimed);
        Assert.Equal(4, dispatcher.Calls);
    }

    [Fact]
    public async Task StopsStartingBatchesOnceTheBudgetIsUsed()
    {
        var time = new FakeTimeProvider();
        var dispatcher = new ScriptedDispatcher(time, TimeSpan.FromSeconds(10), _ => 100);

        var claimed = await dispatcher.DispatchPendingAsync(TimeSpan.FromSeconds(50), time, default);

        Assert.Equal(5, dispatcher.Calls);
        Assert.Equal(500, claimed);
    }

    [Fact]
    public async Task CancelledToken_DispatchesNothing()
    {
        var time = new FakeTimeProvider();
        var dispatcher = new ScriptedDispatcher(time, TimeSpan.FromSeconds(1), _ => 100);

        var claimed = await dispatcher.DispatchPendingAsync(TimeSpan.FromSeconds(50), time, new CancellationToken(canceled: true));

        Assert.Equal(0, claimed);
        Assert.Equal(0, dispatcher.Calls);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task NonPositiveBudget_IsRejected(int seconds)
    {
        var dispatcher = new ScriptedDispatcher(new FakeTimeProvider(), TimeSpan.Zero, _ => 0);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => dispatcher.DispatchPendingAsync(TimeSpan.FromSeconds(seconds), default));
    }

    [Fact]
    public async Task DrainsARealOutbox()
    {
        await using var host = FunctionsHost.Create(b => b.Route<OrderPlaced>().To("orders"));
        await host.SendAsync(Enumerable.Range(1, 5).Select(i => new OrderPlaced(i)));

        var claimed = await host.Dispatcher.DispatchPendingAsync(default);

        Assert.Equal(5, claimed);
        Assert.Equal(5, host.Harness.Sent<OrderPlaced>().Count);
    }
}
