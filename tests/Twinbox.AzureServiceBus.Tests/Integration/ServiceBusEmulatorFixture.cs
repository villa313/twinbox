using Testcontainers.ServiceBus;

namespace Twinbox.AzureServiceBus.Tests.Integration;

public sealed class ServiceBusEmulatorFixture : IAsyncLifetime
{
    private readonly ServiceBusContainer _container = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
        .WithAcceptLicenseAgreement(true)
        .WithConfig(Path.Combine(AppContext.BaseDirectory, "emulator-config.json"))
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync() => await _container.StartAsync().ConfigureAwait(false);

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}
