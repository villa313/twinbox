using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.RedisStreams.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task UseRedisStreams_RegistersTransportAndConsumer()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseRedisStreams(o =>
            {
                o.Configuration = "localhost:6379";
                o.Listen("orders", "billing").Listen("orders", "shipping");
            }))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(RedisStreamsTransport.TransportName, transport.Name);
        Assert.Equal("redisstreams", transport.Name);
        Assert.Same(services.GetRequiredService<RedisStreamsTransport>(), transport);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is RedisStreamsConsumerService);
        Assert.Equal(
            [new RedisStreamsListener("orders", "billing"), new RedisStreamsListener("orders", "shipping")],
            services.GetRequiredService<IOptions<RedisStreamsOptions>>().Value.Listeners);
    }

    [Fact]
    public async Task ConfigurationOverload_ConfiguresIt()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseRedisStreams("redis-1:6379,password=secret"))
            .BuildServiceProvider();

        Assert.Equal("redis-1:6379,password=secret", services.GetRequiredService<IOptions<RedisStreamsOptions>>().Value.Configuration);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void ConfigurationOverload_RejectsBlankConfiguration(string configuration) =>
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UseRedisStreams(configuration)));

    [Fact]
    public async Task ConnectionFactory_StandsInForConfiguration()
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseRedisStreams(o => o.ConnectionFactory = _ => throw new InvalidOperationException()))
            .BuildServiceProvider();

        Assert.NotNull(services.GetRequiredService<IOptions<RedisStreamsOptions>>().Value.ConnectionFactory);
    }

    [Fact]
    public void Defaults_MatchTheDocumentedValues()
    {
        var options = new RedisStreamsOptions();

        Assert.Equal(TimeSpan.FromMinutes(1), options.ClaimIdleAfter);
        Assert.Equal(10, options.MaxDeliveryAttempts);
        Assert.Null(options.MaxLength);
        Assert.Equal(Environment.MachineName, options.ConsumerName);
        Assert.Equal(TimeSpan.FromHours(1), options.RemoveIdleConsumersAfter);
    }

    [Theory]
    [InlineData(nameof(RedisStreamsOptions.Configuration))]
    [InlineData(nameof(RedisStreamsOptions.MaxLength))]
    [InlineData(nameof(RedisStreamsOptions.ClaimIdleAfter))]
    [InlineData(nameof(RedisStreamsOptions.MaxDeliveryAttempts))]
    [InlineData(nameof(RedisStreamsOptions.BatchSize))]
    [InlineData(nameof(RedisStreamsOptions.PollInterval))]
    [InlineData(nameof(RedisStreamsOptions.ConsumerName))]
    [InlineData(nameof(RedisStreamsOptions.RemoveIdleConsumersAfter))]
    public async Task InvalidSetting_FailsValidation(string setting)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseRedisStreams(o =>
            {
                o.Configuration = "localhost:6379";
                Break(o, setting);
            }))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<RedisStreamsOptions>>().Value);
    }

    [Fact]
    public async Task ConfigurationOverload_CanStillRegisterListeners()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseRedisStreams("localhost:6379", o => o.Listen("orders", "billing")))
            .BuildServiceProvider();

        var options = services.GetRequiredService<IOptions<RedisStreamsOptions>>().Value;
        Assert.Equal("localhost:6379", options.Configuration);
        Assert.Equal([new RedisStreamsListener("orders", "billing")], options.Listeners);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Listen_RejectsBlankNames(string name)
    {
        var options = new RedisStreamsOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.Listen(name, "group"));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("stream", name));
    }

    [Fact]
    public void Listener_DeadStreamSitsBesideItsStream() =>
        Assert.Equal("orders:dead", new RedisStreamsListener("orders", "billing").DeadStream);

    [Fact]
    public void ParseConfiguration_KeepsRetryingWhenTheServerIsDownUnlessTold()
    {
        Assert.False(RedisStreamsConnection.ParseConfiguration("localhost:6379").AbortOnConnectFail);
        Assert.True(RedisStreamsConnection.ParseConfiguration("localhost:6379,abortConnect=true").AbortOnConnectFail);
    }

    [Fact]
    public void SweepInterval_IsHalfTheClaimThreshold() =>
        Assert.Equal(
            TimeSpan.FromSeconds(30),
            RedisStreamsConsumerService.SweepInterval(new RedisStreamsOptions { ClaimIdleAfter = TimeSpan.FromMinutes(1) }));

    private static void Break(RedisStreamsOptions options, string setting)
    {
        switch (setting)
        {
            case nameof(RedisStreamsOptions.Configuration):
                options.Configuration = " ";
                break;
            case nameof(RedisStreamsOptions.MaxLength):
                options.MaxLength = 0;
                break;
            case nameof(RedisStreamsOptions.ClaimIdleAfter):
                options.ClaimIdleAfter = TimeSpan.Zero;
                break;
            case nameof(RedisStreamsOptions.MaxDeliveryAttempts):
                options.MaxDeliveryAttempts = 0;
                break;
            case nameof(RedisStreamsOptions.BatchSize):
                options.BatchSize = 0;
                break;
            case nameof(RedisStreamsOptions.PollInterval):
                options.PollInterval = TimeSpan.Zero;
                break;
            case nameof(RedisStreamsOptions.ConsumerName):
                options.ConsumerName = " ";
                break;
            case nameof(RedisStreamsOptions.RemoveIdleConsumersAfter):
                options.RemoveIdleConsumersAfter = options.ClaimIdleAfter / 2;
                break;
        }
    }
}
