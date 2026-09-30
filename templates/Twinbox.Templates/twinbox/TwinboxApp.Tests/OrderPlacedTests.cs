using Microsoft.Extensions.DependencyInjection;
using Twinbox;
using Twinbox.InMemory;
using Xunit;

namespace TwinboxApp.Tests;

public sealed class OrderPlacedTests
{
    [Fact]
    public async Task OrderPlaced_is_delivered_without_dead_letters()
    {
        var cancellationToken = TestContext.Current.CancellationToken;

        // The harness swaps in the in-memory store and transport and runs nothing in the background.
        var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(twinbox => twinbox
                .UseTestHarness()
                .Route<OrderPlaced>().To("orders")
                .AddHandler<OrderPlacedHandler, OrderPlaced>());
        await using var provider = services.BuildServiceProvider();

        var orderId = Guid.NewGuid();
        await using (var scope = provider.CreateAsyncScope())
        {
            scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(orderId, 42.50m));
            await scope.ServiceProvider.GetRequiredService<InMemoryUnitOfWork>().CommitAsync(cancellationToken);
        }

        var harness = provider.GetTwinboxHarness();
        await harness.DrainAsync(cancellationToken);

        var sent = Assert.Single(harness.Sent<OrderPlaced>());
        Assert.Equal(orderId, sent.OrderId);
        Assert.Empty(harness.DeadLetteredOutgoing());
    }
}
