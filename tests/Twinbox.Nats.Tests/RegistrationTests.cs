using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream.Models;
using Twinbox.Transport;

namespace Twinbox.Nats.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task UseNats_RegistersTransportAndConsumer()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseNats(o => o
                .Listen("ORDERS", "billing")
                .Listen("ORDERS", "audit", "orders.*.placed")))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(NatsTransport.TransportName, transport.Name);
        Assert.Equal("nats", transport.Name);
        Assert.Same(services.GetRequiredService<NatsTransport>(), transport);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is NatsConsumerService);
        Assert.Equal(
            [new NatsListener("ORDERS", "billing", null), new NatsListener("ORDERS", "audit", "orders.*.placed")],
            services.GetRequiredService<IOptions<NatsOptions>>().Value.Listeners);
    }

    [Fact]
    public async Task UrlOverload_ConfiguresIt()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseNats("nats://a:4222,nats://b:4222"))
            .BuildServiceProvider();

        Assert.Equal("nats://a:4222,nats://b:4222", services.GetRequiredService<IOptions<NatsOptions>>().Value.Url);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void UrlOverload_RejectsBlankUrls(string url) =>
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UseNats(url)));

    [Theory]
    [MemberData(nameof(InvalidSettings))]
    public async Task InvalidSettings_FailValidation(string name)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseNats(InvalidSettingsByName[name]))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<NatsOptions>>().Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Listen_RejectsBlankNames(string name)
    {
        var options = new NatsOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.Listen(name, "billing"));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("ORDERS", name));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("ORDERS", "billing", name));
    }

    [Fact]
    public void AddStream_NeedsSubjects()
    {
        var options = new NatsOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.AddStream("ORDERS"));
        Assert.ThrowsAny<ArgumentException>(() => options.AddStream("ORDERS", "orders.>", " "));
        Assert.ThrowsAny<ArgumentException>(() => options.AddStream(" ", "orders.>"));
    }

    [Fact]
    public void ConsumerConfig_IsDurableWithExplicitAcksAndTheConfiguredLimits()
    {
        var options = new NatsOptions { AckWait = TimeSpan.FromSeconds(12), MaxDeliveryAttempts = 4, MaxAckPending = 64 };

        var config = NatsClients.CreateConsumerConfig(options, new NatsListener("ORDERS", "billing", "orders.>"));

        Assert.Equal("billing", config.DurableName);
        Assert.Equal("billing", config.Name);
        Assert.Equal(ConsumerConfigAckPolicy.Explicit, config.AckPolicy);
        Assert.Equal(TimeSpan.FromSeconds(12), config.AckWait);
        Assert.Equal(-1, config.MaxDeliver);
        Assert.Equal(64, config.MaxAckPending);
        Assert.Equal("orders.>", config.FilterSubject);
    }

    [Fact]
    public void StreamConfig_UsesTheDuplicateWindow()
    {
        var options = new NatsOptions { DuplicateWindow = TimeSpan.FromMinutes(10) };

        var config = NatsClients.CreateStreamConfig(options, new NatsStream("ORDERS", ["orders.>", "refunds.*"]));

        Assert.Equal("ORDERS", config.Name);
        Assert.Equal(["orders.>", "refunds.*"], config.Subjects!);
        Assert.Equal(TimeSpan.FromMinutes(10), config.DuplicateWindow);
    }

    [Fact]
    public void ConnectionOptions_ApplyUrlNameAndTheHook()
    {
        var options = new NatsOptions
        {
            Url = "nats://broker:4222",
            ClientName = "billing",
            ConfigureConnection = o => o with { AuthOpts = NatsAuthOpts.Default with { Token = "secret" } },
        };

        var opts = NatsClients.CreateConnectionOptions(options, NullLoggerFactory.Instance);

        Assert.Equal("nats://broker:4222", opts.Url);
        Assert.Equal("billing", opts.Name);
        Assert.Equal("secret", opts.AuthOpts.Token);
        Assert.Same(NullLoggerFactory.Instance, opts.LoggerFactory);
    }

    public static TheoryData<string> InvalidSettings() => [.. InvalidSettingsByName.Keys];

    private static readonly Dictionary<string, Action<NatsOptions>> InvalidSettingsByName = new()
    {
        ["blank url"] = o => o.Url = " ",
        ["zero ack wait"] = o => o.AckWait = TimeSpan.Zero,
        ["zero max delivery attempts"] = o => o.MaxDeliveryAttempts = 0,
        ["zero max ack pending"] = o => o.MaxAckPending = 0,
        ["zero prefetch"] = o => o.PrefetchCount = 0,
        ["zero concurrency"] = o => o.ConsumerConcurrency = 0,
        ["zero duplicate window"] = o => o.DuplicateWindow = TimeSpan.Zero,
        ["retry delay over its cap"] = o => o.RetryDelay = TimeSpan.FromMinutes(1),
        ["blank dead-letter subject"] = o => o.DeadLetterSubject = " ",
    };
}
