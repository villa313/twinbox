using DotPulsar;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.Pulsar.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task UsePulsar_RegistersTransportAndConsumer()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UsePulsar(o =>
            {
                o.ServiceUrl = new Uri("pulsar://localhost:6650");
                o.Listen("orders", "billing").Listen("orders", "shipping");
            }))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(PulsarTransport.TransportName, transport.Name);
        Assert.Equal("pulsar", transport.Name);
        Assert.Same(services.GetRequiredService<PulsarTransport>(), transport);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is PulsarConsumerService);
        Assert.Equal(
            [new PulsarListener("orders", "billing"), new PulsarListener("orders", "shipping")],
            services.GetRequiredService<IOptions<PulsarOptions>>().Value.Listeners);
    }

    [Fact]
    public async Task ServiceUrlOverload_ConfiguresIt()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UsePulsar("pulsar+ssl://broker-1:6651"))
            .BuildServiceProvider();

        Assert.Equal(new Uri("pulsar+ssl://broker-1:6651"), services.GetRequiredService<IOptions<PulsarOptions>>().Value.ServiceUrl);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void ServiceUrlOverload_RejectsBlankAddresses(string serviceUrl) =>
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UsePulsar(serviceUrl)));

    [Fact]
    public void ServiceUrlOverload_RejectsRelativeAddresses() =>
        Assert.Throws<UriFormatException>(() => new ServiceCollection().AddTwinbox(b => b.UsePulsar("broker/6650")));

    [Fact]
    public void Defaults_MatchTheDocumentedValues()
    {
        var options = new PulsarOptions();

        Assert.Equal(SubscriptionType.Shared, options.SubscriptionType);
        Assert.Equal(SubscriptionInitialPosition.Earliest, options.InitialPosition);
        Assert.Equal(10, options.MaxRedeliveryCount);
        Assert.Equal(TimeSpan.FromMinutes(1), options.NegativeAckRedeliveryDelay);
        Assert.Equal("-dlq", options.DeadLetterSuffix);
        Assert.Equal(1, options.ConsumerConcurrency);
        Assert.Equal(TimeSpan.FromSeconds(30), options.SendTimeout);
    }

    [Fact]
    public async Task ConfigureClient_HasTheLastSayOverTheClient()
    {
        var configured = false;
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UsePulsar(o =>
            {
                o.ServiceUrl = new Uri("pulsar://localhost:6650");
                o.ConfigureClient = client => configured = client is not null;
            }))
            .BuildServiceProvider();

        services.GetRequiredService<PulsarClients>().CreateClientBuilder();

        Assert.True(configured);
    }

    [Theory]
    [InlineData(nameof(PulsarOptions.ServiceUrl))]
    [InlineData("ServiceUrlScheme")]
    [InlineData(nameof(PulsarOptions.SubscriptionType))]
    [InlineData(nameof(PulsarOptions.InitialPosition))]
    [InlineData(nameof(PulsarOptions.MaxRedeliveryCount))]
    [InlineData(nameof(PulsarOptions.NegativeAckRedeliveryDelay))]
    [InlineData(nameof(PulsarOptions.DeadLetterSuffix))]
    [InlineData(nameof(PulsarOptions.ConsumerConcurrency))]
    [InlineData("ConcurrentExclusive")]
    [InlineData(nameof(PulsarOptions.SendTimeout))]
    public async Task InvalidSetting_FailsValidation(string setting)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UsePulsar(o =>
            {
                o.ServiceUrl = new Uri("pulsar://localhost:6650");
                Break(o, setting);
            }))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<PulsarOptions>>().Value);
    }

    [Theory]
    [InlineData(SubscriptionType.Shared)]
    [InlineData(SubscriptionType.KeyShared)]
    public async Task ConcurrentConsumers_AreAllowedOnSharedSubscriptions(SubscriptionType type)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UsePulsar(o =>
            {
                o.ServiceUrl = new Uri("pulsar://localhost:6650");
                o.SubscriptionType = type;
                o.ConsumerConcurrency = 4;
                o.MaxRedeliveryCount = 0;
                o.NegativeAckRedeliveryDelay = TimeSpan.Zero;
            }))
            .BuildServiceProvider();

        Assert.Equal(4, services.GetRequiredService<IOptions<PulsarOptions>>().Value.ConsumerConcurrency);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Listen_RejectsBlankNames(string name)
    {
        var options = new PulsarOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.Listen(name, "subscription"));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("topic", name));
    }

    private static void Break(PulsarOptions options, string setting)
    {
        switch (setting)
        {
            case nameof(PulsarOptions.ServiceUrl):
                options.ServiceUrl = null;
                break;
            case "ServiceUrlScheme":
                options.ServiceUrl = new Uri("http://localhost:8080");
                break;
            case nameof(PulsarOptions.SubscriptionType):
                options.SubscriptionType = (SubscriptionType)42;
                break;
            case nameof(PulsarOptions.InitialPosition):
                options.InitialPosition = (SubscriptionInitialPosition)42;
                break;
            case nameof(PulsarOptions.MaxRedeliveryCount):
                options.MaxRedeliveryCount = -1;
                break;
            case nameof(PulsarOptions.NegativeAckRedeliveryDelay):
                options.NegativeAckRedeliveryDelay = TimeSpan.FromMilliseconds(-1);
                break;
            case nameof(PulsarOptions.DeadLetterSuffix):
                options.DeadLetterSuffix = " ";
                break;
            case nameof(PulsarOptions.ConsumerConcurrency):
                options.ConsumerConcurrency = 0;
                break;
            case "ConcurrentExclusive":
                options.SubscriptionType = SubscriptionType.Failover;
                options.ConsumerConcurrency = 2;
                break;
            case nameof(PulsarOptions.SendTimeout):
                options.SendTimeout = TimeSpan.Zero;
                break;
        }
    }
}
