using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Twinbox.Maintenance;
using Twinbox.Tenancy;

namespace Twinbox.UnitTests;

public sealed class MaintenanceTests
{
    [Fact]
    public async Task DispatchPending_DrainsBatchesUntilTheOutboxIsEmpty()
    {
        var time = new FakeTimeProvider();
        int[] batches = [100, 100, 30];
        var dispatcher = new ScriptedDispatcher(time, TimeSpan.FromSeconds(1), call => call < batches.Length ? batches[call] : 0);

        var claimed = await Maintenance(dispatcher, time).DispatchPendingAsync(TimeSpan.FromSeconds(50), default);

        Assert.Equal(230, claimed);
        Assert.Equal(4, dispatcher.Calls);
    }

    [Fact]
    public async Task DispatchPending_StopsStartingBatchesOnceTheBudgetIsUsed()
    {
        var time = new FakeTimeProvider();
        var dispatcher = new ScriptedDispatcher(time, TimeSpan.FromSeconds(10), _ => 100);

        var claimed = await Maintenance(dispatcher, time).DispatchPendingAsync(TimeSpan.FromSeconds(50), default);

        Assert.Equal(5, dispatcher.Calls);
        Assert.Equal(500, claimed);
    }

    [Fact]
    public async Task DispatchPending_WithInfiniteBudget_RunsUntilEmpty()
    {
        var time = new FakeTimeProvider();
        var dispatcher = new ScriptedDispatcher(time, TimeSpan.FromMinutes(10), call => call < 20 ? 1 : 0);

        var claimed = await Maintenance(dispatcher, time).DispatchPendingAsync(Timeout.InfiniteTimeSpan, default);

        Assert.Equal(20, claimed);
    }

    [Fact]
    public async Task DispatchPending_WithCancelledToken_DispatchesNothing()
    {
        var time = new FakeTimeProvider();
        var dispatcher = new ScriptedDispatcher(time, TimeSpan.FromSeconds(1), _ => 100);

        var claimed = await Maintenance(dispatcher, time).DispatchPendingAsync(TimeSpan.FromSeconds(50), new CancellationToken(canceled: true));

        Assert.Equal(0, claimed);
        Assert.Equal(0, dispatcher.Calls);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public async Task DispatchPending_RejectsNonPositiveBudgets(int seconds)
    {
        var time = new FakeTimeProvider();
        var maintenance = Maintenance(new ScriptedDispatcher(time, TimeSpan.Zero, _ => 0), time);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => maintenance.DispatchPendingAsync(TimeSpan.FromSeconds(seconds), default));
    }

    [Fact]
    public async Task AnyHost_CanDispatchAndCleanUpOnDemand()
    {
        await using var host = TestHost.Create(b => b
            .Route<OrderPlaced>().To("orders")
            .Configure(o => o.Retention.Enabled = false));
        await host.SendAsync(o =>
        {
            o.Send(new OrderPlaced(1));
            o.Send(new OrderPlaced(2));
        });
        var maintenance = host.Services.GetRequiredService<ITwinboxMaintenance>();

        Assert.Equal(2, await maintenance.DispatchPendingAsync(Timeout.InfiniteTimeSpan, default));
        Assert.Equal(0, await maintenance.RunCleanupAsync(default));

        host.Time.Advance(TimeSpan.FromDays(2));
        Assert.Equal(2, await maintenance.RunCleanupAsync(default));
        Assert.Empty(host.Harness.Store.Snapshot());
    }

    private static TwinboxMaintenance Maintenance(IOutboxDispatcher dispatcher, TimeProvider time)
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var tenants = new TenantDirectory(new TwinboxScopeFactory(services.GetRequiredService<IServiceScopeFactory>()), time);
        return new TwinboxMaintenance(dispatcher, [], tenants, Options.Create(new TwinboxOptions()), time);
    }

    private sealed class ScriptedDispatcher(FakeTimeProvider time, TimeSpan batchDuration, Func<int, int> claimedOnCall) : IOutboxDispatcher
    {
        public int Calls { get; private set; }

        public Task<int> DispatchBatchAsync(CancellationToken cancellationToken)
        {
            time.Advance(batchDuration);
            return Task.FromResult(claimedOnCall(Calls++));
        }
    }
}
