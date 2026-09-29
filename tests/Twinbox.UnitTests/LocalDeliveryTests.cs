using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.UnitTests;

public sealed class LocalDeliveryTests
{
    [Fact]
    public async Task Dispatch_DeliversToLocalHandlers()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal);

        await host.SendAsync(o => o.Send(new OrderPlaced(8)));
        await host.Services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default);

        Assert.Equal(["OrderPlacedHandler:8"], journal.Entries);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Harness.Store.Snapshot()).Status);
    }

    [Fact]
    public async Task FailingHandler_IsRetriedFromTheOutboxWithItsAttemptNumber()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal, b => b.AddHandler<InboxTests.FlakyOrderPlacedHandler, OrderPlaced>());
        var dispatcher = host.Services.GetRequiredService<IOutboxDispatcher>();

        await host.SendAsync(o => o.Send(new OrderPlaced(2)));
        await dispatcher.DispatchBatchAsync(default);
        Assert.Equal(OutboxMessageStatus.Pending, Assert.Single(host.Harness.Store.Snapshot()).Status);

        host.Time.Advance(TimeSpan.FromMinutes(5));
        await dispatcher.DispatchBatchAsync(default);

        // The first handler succeeded on attempt 1, so the inbox keeps it from running twice.
        Assert.Equal(["OrderPlacedHandler:2", "FlakyOrderPlacedHandler:2"], journal.Entries);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Harness.Store.Snapshot()).Status);
    }

    private static TestHost CreateHost(InboxTests.Journal journal, Action<TwinboxBuilder>? configure = null) =>
        TestHost.Create(
            b =>
            {
                b.UseLocalDelivery()
                    .Route<OrderPlaced>().To("domain-events", transport: "local")
                    .AddHandler<InboxTests.OrderPlacedHandler, OrderPlaced>();
                configure?.Invoke(b);
            },
            s => s.AddSingleton(journal));
}
