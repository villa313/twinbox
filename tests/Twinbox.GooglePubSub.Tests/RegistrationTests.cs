using Google.Api.Gax;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.GooglePubSub.Tests;

public sealed class RegistrationTests
{
    private static readonly Dictionary<string, Action<GooglePubSubOptions>> InvalidSettings = new()
    {
        ["no-project"] = o => o.ProjectId = " ",
        ["blank-emulator"] = o => o.EmulatorHost = " ",
        ["ack-too-short"] = o => o.AckDeadline = TimeSpan.FromSeconds(9),
        ["ack-too-long"] = o => o.AckDeadline = TimeSpan.FromSeconds(601),
        ["no-outstanding"] = o => o.MaxOutstandingMessages = 0,
        ["too-few-attempts"] = o => o.MaxDeliveryAttempts = 4,
        ["too-many-attempts"] = o => o.MaxDeliveryAttempts = 101,
        ["blank-dead-letter-topic"] = o => o.DeadLetterTopic = " ",
    };

    public static TheoryData<string> InvalidOptions => [.. InvalidSettings.Keys];

    [Fact]
    public async Task UseGooglePubSub_RegistersTransportAndSubscriber()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseGooglePubSub(o =>
            {
                o.ProjectId = "p1";
                o.Subscribe("billing", "orders").Subscribe("shipping", "projects/other/topics/orders");
            }))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(GooglePubSubTransport.TransportName, transport.Name);
        Assert.Equal("googlepubsub", transport.Name);
        Assert.Same(services.GetRequiredService<GooglePubSubTransport>(), transport);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is GooglePubSubSubscriberService);
        Assert.Equal(
            [new GooglePubSubSubscription("billing", "orders"), new GooglePubSubSubscription("shipping", "projects/other/topics/orders")],
            services.GetRequiredService<IOptions<GooglePubSubOptions>>().Value.Subscriptions);
    }

    [Fact]
    public async Task ProjectIdOverload_ConfiguresIt()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseGooglePubSub("my-project"))
            .BuildServiceProvider();

        Assert.Equal("my-project", services.GetRequiredService<IOptions<GooglePubSubOptions>>().Value.ProjectId);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void ProjectIdOverload_RejectsBlankIds(string projectId) =>
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UseGooglePubSub(projectId)));

    [Theory]
    [MemberData(nameof(InvalidOptions))]
    public async Task InvalidOptions_FailValidation(string setting)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseGooglePubSub(o =>
            {
                o.ProjectId = "p1";
                InvalidSettings[setting](o);
            }))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<GooglePubSubOptions>>().Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void SubscriptionNames_RejectBlanks(string name)
    {
        var options = new GooglePubSubOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.Subscribe(name, "topic"));
        Assert.ThrowsAny<ArgumentException>(() => options.Subscribe("subscription", name));
    }

    [Fact]
    public void EmulatorHost_TurnsOffEnvironmentDetection()
    {
        Assert.Equal(EmulatorDetection.EmulatorOrProduction, Clients(new GooglePubSubOptions { ProjectId = "p1" }).EmulatorMode);
        Assert.Equal(EmulatorDetection.None, Clients(new GooglePubSubOptions { ProjectId = "p1", EmulatorHost = "localhost:8085" }).EmulatorMode);
    }

    [Fact]
    public async Task MalformedDestination_IsPermanentFailure()
    {
        await using var clients = Clients(new GooglePubSubOptions { ProjectId = "p1", EmulatorHost = "localhost:1" });
        var transport = new GooglePubSubTransport(clients);
        var message = new TransportMessage("m1", "order-placed", "projects/p1/queues/orders", "{}"u8.ToArray(), "application/json", new Dictionary<string, string>(), null);

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(() => transport.SendAsync(message, TestContext.Current.CancellationToken));

        Assert.Contains("not a valid Pub/Sub topic", error.Message, StringComparison.Ordinal);
    }

    internal static GooglePubSubClients Clients(GooglePubSubOptions options) =>
        new(Options.Create(options), Microsoft.Extensions.Logging.Abstractions.NullLogger<GooglePubSubClients>.Instance);
}
