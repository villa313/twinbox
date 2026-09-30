using Azure.Messaging.ServiceBus;
using Testcontainers.ServiceBus;

namespace Twinbox.AzureServiceBus.Tests.Integration;

public sealed class ServiceBusEmulatorFixture : IAsyncLifetime
{
    private static readonly string[] Queues = ["orders", "poison"];
    private static readonly TimeSpan ReadinessTimeout = TimeSpan.FromMinutes(2);

    private readonly ServiceBusContainer _container = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
        .WithAcceptLicenseAgreement(true)
        .WithConfig(Path.Combine(AppContext.BaseDirectory, "emulator-config.json"))
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync().ConfigureAwait(false);
        await WaitUntilEntitiesAnswerAsync().ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => _container.DisposeAsync();

    // Under Docker load the container reports ready before the emulator has loaded its entities.
    private async Task WaitUntilEntitiesAnswerAsync()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        var deadline = DateTime.UtcNow + ReadinessTimeout;
        foreach (var queue in Queues)
        {
            await using var receiver = client.CreateReceiver(queue);
            while (true)
            {
                try
                {
                    await receiver.PeekMessageAsync().ConfigureAwait(false);
                    break;
                }
                catch (Exception) when (DateTime.UtcNow < deadline)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
                }
            }
        }
    }
}
