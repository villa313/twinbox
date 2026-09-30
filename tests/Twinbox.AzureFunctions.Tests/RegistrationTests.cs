using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Twinbox.AzureFunctions.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task UseAzureFunctions_RegistersTheHelpers()
    {
        await using var host = FunctionsHost.Create();

        Assert.NotNull(host.Services.GetRequiredService<TwinboxServiceBusTrigger>());
        Assert.NotNull(host.Services.GetRequiredService<ITwinboxMaintenance>());
    }

    [Fact]
    public async Task UseAzureFunctions_TurnsOffBackgroundDispatchAndRetention()
    {
        await using var host = FunctionsHost.Create();

        var options = host.Services.GetRequiredService<IOptions<TwinboxOptions>>().Value;
        Assert.False(options.Dispatcher.Enabled);
        Assert.False(options.Retention.Enabled);

        var background = await StartTwinboxServicesAsync(host.Services);
        Assert.NotEmpty(background);
        foreach (var service in background)
        {
            // Disabled services return straight away, but on a busy machine not necessarily before StartAsync does.
            await Assert.IsAssignableFrom<Task>(service.ExecuteTask).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task Configuration_CanTurnBackgroundDispatchBackOn()
    {
        await using var host = FunctionsHost.Create(settings: new() { ["Twinbox:Dispatcher:Enabled"] = "true" });

        Assert.True(host.Services.GetRequiredService<IOptions<TwinboxOptions>>().Value.Dispatcher.Enabled);

        var background = await StartTwinboxServicesAsync(host.Services);
        try
        {
            Assert.Contains(background, s => s.ExecuteTask is { IsCompleted: false });
        }
        finally
        {
            foreach (var service in background)
            {
                await service.StopAsync(default);
            }
        }
    }

    [Fact]
    public async Task Cleanup_PurgesSentMessagesAndOldInboxEntries()
    {
        await using var host = FunctionsHost.Create(b => b
            .Route<OrderPlaced>().To("orders")
            .AddHandler<OrderPlacedHandler, OrderPlaced>());
        await host.SendAsync([new OrderPlaced(1)]);
        await host.Maintenance.DispatchPendingAsync(default);
        await host.Services.GetRequiredService<TwinboxServiceBusTrigger>().ProcessServiceBusMessageAsync(
            ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("""{"orderId":2}"""), messageId: "in-1", subject: "OrderPlaced"),
            new RecordingMessageActions(),
            default);
        var maintenance = host.Maintenance;

        Assert.Equal(0, await maintenance.RunCleanupAsync(default));

        host.Time.Advance(TimeSpan.FromDays(8));
        Assert.Equal(2, await maintenance.RunCleanupAsync(default));
        Assert.Empty(host.Harness.Store.Snapshot());
    }

    [Fact]
    public async Task DispatchPending_DrainsTheOutboxWithinTheDefaultBudget()
    {
        await using var host = FunctionsHost.Create(b => b.Route<OrderPlaced>().To("orders"));
        await host.SendAsync(Enumerable.Range(1, 5).Select(i => new OrderPlaced(i)));

        var claimed = await host.Maintenance.DispatchPendingAsync(default);

        Assert.Equal(5, claimed);
        Assert.Equal(5, host.Harness.Sent<OrderPlaced>().Count);
    }

    private static async Task<List<BackgroundService>> StartTwinboxServicesAsync(IServiceProvider services)
    {
        var background = services.GetServices<IHostedService>()
            .OfType<BackgroundService>()
            .Where(s => s.GetType().Namespace == "Twinbox.Hosting")
            .ToList();
        foreach (var service in background)
        {
            await service.StartAsync(default);
        }

        return background;
    }
}
