using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.Kafka.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task UseKafka_RegistersTransportAndConsumer()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseKafka(o =>
            {
                o.BootstrapServers = "localhost:9092";
                o.Listen("orders", "billing").Listen("payments", "billing");
            }))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(KafkaTransport.TransportName, transport.Name);
        Assert.Same(services.GetRequiredService<KafkaTransport>(), transport);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is KafkaConsumerService);
        Assert.Equal(
            [new KafkaListener("orders", "billing"), new KafkaListener("payments", "billing")],
            services.GetRequiredService<IOptions<KafkaOptions>>().Value.Listeners);
    }

    [Fact]
    public async Task BootstrapServersOverload_ConfiguresThem()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseKafka("broker-1:9092,broker-2:9092"))
            .BuildServiceProvider();

        Assert.Equal("broker-1:9092,broker-2:9092", services.GetRequiredService<IOptions<KafkaOptions>>().Value.BootstrapServers);
    }

    [Fact]
    public async Task BootstrapServersOverload_CanStillRegisterListeners()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseKafka("localhost:9092", o => o.Listen("orders", "billing")))
            .BuildServiceProvider();

        var options = services.GetRequiredService<IOptions<KafkaOptions>>().Value;
        Assert.Equal("localhost:9092", options.BootstrapServers);
        Assert.Equal([new KafkaListener("orders", "billing")], options.Listeners);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void BootstrapServersOverload_RejectsBlankServers(string servers) =>
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UseKafka(servers)));

    [Fact]
    public async Task MissingBootstrapServers_FailsValidation()
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseKafka(_ => { }))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<KafkaOptions>>().Value);
    }

    [Fact]
    public async Task RetryDelayLongerThanItsCap_FailsValidation()
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseKafka(o =>
            {
                o.BootstrapServers = "localhost:9092";
                o.RetryDelay = TimeSpan.FromMinutes(1);
            }))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<KafkaOptions>>().Value);
    }

    [Fact]
    public void Batching_IsOffByDefault()
    {
        var options = new KafkaOptions();

        Assert.Equal(1, options.MaxBatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(50), options.MaxBatchWait);
    }

    [Theory]
    [InlineData(0, 50)]
    [InlineData(-1, 50)]
    [InlineData(10, -1)]
    public async Task InvalidBatchSettings_FailValidation(int maxBatchSize, int maxBatchWaitMilliseconds)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseKafka(o =>
            {
                o.BootstrapServers = "localhost:9092";
                o.MaxBatchSize = maxBatchSize;
                o.MaxBatchWait = TimeSpan.FromMilliseconds(maxBatchWaitMilliseconds);
            }))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<KafkaOptions>>().Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Listen_RejectsBlankNames(string name)
    {
        var options = new KafkaOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.Listen(name, "group"));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("topic", name));
    }

    [Fact]
    public void ProducerConfig_IsIdempotentAndWaitsForAllReplicas()
    {
        var options = new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            SendTimeout = TimeSpan.FromSeconds(12),
            ConfigureClient = c => c.SecurityProtocol = SecurityProtocol.SaslSsl,
            ConfigureProducer = c => c.LingerMs = 7,
        };

        var config = KafkaClients.CreateProducerConfig(options);

        Assert.True(config.EnableIdempotence);
        Assert.Equal(Acks.All, config.Acks);
        Assert.Equal(12_000, config.MessageTimeoutMs);
        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(7, config.LingerMs);
    }

    [Fact]
    public void ConsumerConfig_LeavesCommittingToTheTransport()
    {
        var options = new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Latest,
            ConfigureClient = c => c.SaslUsername = "app",
        };

        var config = KafkaClients.CreateConsumerConfig(options, new KafkaListener("orders", "billing"));

        Assert.Equal("billing", config.GroupId);
        Assert.False(config.EnableAutoCommit);
        Assert.False(config.EnableAutoOffsetStore);
        Assert.Equal(AutoOffsetReset.Latest, config.AutoOffsetReset);
        Assert.Equal("app", config.SaslUsername);
    }

    [Theory]
    [InlineData(1, 100)]
    [InlineData(2, 200)]
    [InlineData(4, 800)]
    [InlineData(5, 1000)]
    [InlineData(500, 1000)]
    public void RetryDelay_DoublesUpToItsCap(int attempt, int expectedMilliseconds)
    {
        var options = new KafkaOptions { RetryDelay = TimeSpan.FromMilliseconds(100), MaxRetryDelay = TimeSpan.FromSeconds(1) };

        Assert.Equal(TimeSpan.FromMilliseconds(expectedMilliseconds), KafkaConsumerService.RetryDelay(options, attempt));
    }
}
