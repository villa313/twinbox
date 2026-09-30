using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.RabbitMQ.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task UseRabbitMQ_RegistersTransportAndConsumer()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseRabbitMQ(o => o.Listen("orders", "sales")))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(RabbitMQTransport.TransportName, transport.Name);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is RabbitMQConsumerService);
        Assert.Equal([new RabbitMQListener("orders", "sales", "#")], services.GetRequiredService<IOptions<RabbitMQOptions>>().Value.Listeners);
    }

    [Fact]
    public async Task ConnectionStringOverload_SetsTheUriAndStillConfigures()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseRabbitMQ("amqps://app:secret@rabbit:5671/sales", o => o.Listen("orders", "sales")))
            .BuildServiceProvider();

        var options = services.GetRequiredService<IOptions<RabbitMQOptions>>().Value;
        Assert.Equal(new Uri("amqps://app:secret@rabbit:5671/sales"), options.ConnectionUri);
        Assert.Single(options.Listeners);
    }

    [Theory]
    [InlineData("")]
    [InlineData("rabbit:5672")]
    [InlineData("http://rabbit:5672")]
    public void ConnectionStringOverload_RejectsAnythingButAnAmqpUri(string connectionString) =>
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UseRabbitMQ(connectionString)));

    [Theory]
    [InlineData(0, 1, 1)]
    [InlineData(65_536, 1, 1)]
    [InlineData(20, 0, 1)]
    [InlineData(20, 65_536, 1)]
    [InlineData(20, 1, 0)]
    public async Task OutOfRangeLimits_FailValidation(int prefetchCount, int consumerConcurrency, int maxDeliveryAttempts)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseRabbitMQ(o =>
            {
                o.PrefetchCount = prefetchCount;
                o.ConsumerConcurrency = consumerConcurrency;
                o.MaxDeliveryAttempts = maxDeliveryAttempts;
            }))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<RabbitMQOptions>>().Value);
    }

    [Fact]
    public async Task RetryDelayLongerThanItsCap_FailsValidation()
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseRabbitMQ(o => o.RetryDelay = TimeSpan.FromMinutes(5)))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<RabbitMQOptions>>().Value);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(10, 30)]
    public void RetryDelay_DoublesPerAttemptUpToItsCap(int attempt, int expectedSeconds)
    {
        var options = new RabbitMQOptions { RetryDelay = TimeSpan.FromSeconds(1), MaxRetryDelay = TimeSpan.FromSeconds(30) };

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), RabbitMQConsumerService.RetryDelay(options, attempt));
    }
}
