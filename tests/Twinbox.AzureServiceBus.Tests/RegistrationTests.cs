using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task UseAzureServiceBus_RegistersTransportAndReceiver()
    {
        var client = new FakeServiceBusClient();
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseAzureServiceBus(_ => client, o => o.Listen("orders").Listen("billing", "invoices")))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(AzureServiceBusTransport.TransportName, transport.Name);
        Assert.Same(client, services.GetRequiredService<AzureServiceBusTransport>().Client);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is AzureServiceBusReceiverService);
        Assert.Equal(
            ["orders", "billing/Subscriptions/invoices"],
            services.GetRequiredService<IOptions<AzureServiceBusOptions>>().Value.Listeners.Select(l => l.EntityPath));
    }

    [Fact]
    public async Task ConnectionStringOverload_BuildsClientFromIt()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseAzureServiceBus(
                "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"))
            .BuildServiceProvider();

        Assert.Equal("localhost", services.GetRequiredService<AzureServiceBusTransport>().Client.FullyQualifiedNamespace);
    }

    [Fact]
    public async Task InvalidConcurrency_FailsValidation()
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseAzureServiceBus(_ => new FakeServiceBusClient(), o => o.MaxConcurrentCalls = 0))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<AzureServiceBusOptions>>().Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Listen_RejectsBlankNames(string name)
    {
        var options = new AzureServiceBusOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.Listen(name));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("topic", name));
    }
}
