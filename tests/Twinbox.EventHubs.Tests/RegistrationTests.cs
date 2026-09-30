using Azure.Core;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.EventHubs.Tests;

public sealed class RegistrationTests
{
    private const string ConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=key;SharedAccessKey=secret;UseDevelopmentEmulator=true";
    private const string StorageConnectionString = "UseDevelopmentStorage=true";

    [Fact]
    public async Task UseEventHubs_RegistersTransportAndProcessors()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseEventHubs(o =>
            {
                o.ConnectionString = ConnectionString;
                o.Listen("orders", "$Default", StorageConnectionString, "checkpoints")
                    .Listen("payments", "billing", new Uri("https://account.blob.core.windows.net/checkpoints"));
            }))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(EventHubsTransport.TransportName, transport.Name);
        Assert.Same(services.GetRequiredService<EventHubsTransport>(), transport);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is EventHubsProcessorService);
        Assert.Equal(
            [("orders", "$Default"), ("payments", "billing")],
            services.GetRequiredService<IOptions<EventHubsOptions>>().Value.Listeners.Select(l => (l.EventHub, l.ConsumerGroup)));
    }

    [Fact]
    public async Task ConnectionStringOverload_ConfiguresIt()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseEventHubs(ConnectionString))
            .BuildServiceProvider();

        Assert.Equal(ConnectionString, services.GetRequiredService<IOptions<EventHubsOptions>>().Value.ConnectionString);
    }

    [Fact]
    public async Task CredentialOverload_ConfiguresNamespaceAndCredential()
    {
        var credential = new FakeCredential();
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseEventHubs("my-namespace.servicebus.windows.net", credential))
            .BuildServiceProvider();

        var options = services.GetRequiredService<IOptions<EventHubsOptions>>().Value;
        Assert.Equal("my-namespace.servicebus.windows.net", options.FullyQualifiedNamespace);
        Assert.Same(credential, options.Credential);
        Assert.Null(options.ConnectionString);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Overloads_RejectBlankArguments(string value)
    {
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UseEventHubs(value)));
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UseEventHubs(value, new FakeCredential())));
    }

    [Fact]
    public async Task MissingConnection_FailsValidation()
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseEventHubs(o => o.FullyQualifiedNamespace = "my-namespace.servicebus.windows.net"))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<EventHubsOptions>>().Value);
    }

    [Theory]
    [InlineData(0, 1000)]
    [InlineData(1, 0)]
    [InlineData(1, 60_000)]
    public async Task InvalidRetryOrBatchSettings_FailValidation(int maxBatchSize, int retryDelayMilliseconds)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseEventHubs(o =>
            {
                o.ConnectionString = ConnectionString;
                o.MaxBatchSize = maxBatchSize;
                o.RetryDelay = TimeSpan.FromMilliseconds(retryDelayMilliseconds);
            }))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<EventHubsOptions>>().Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Listen_RejectsBlankNames(string name)
    {
        var options = new EventHubsOptions();
        var uri = new Uri("https://account.blob.core.windows.net/checkpoints");

        Assert.ThrowsAny<ArgumentException>(() => options.Listen(name, "group", uri));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("orders", name, uri));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("orders", "group", name, "checkpoints"));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("orders", "group", StorageConnectionString, name));
        Assert.Empty(options.Listeners);
    }

    [Fact]
    public void Listen_UsesTheGivenCheckpointContainer()
    {
        var container = new BlobContainerClient(StorageConnectionString, "checkpoints");
        var options = new EventHubsOptions().Listen("orders", "group", container);

        Assert.Same(container, Assert.Single(options.Listeners).CheckpointContainer(options));
    }

    [Fact]
    public void Defaults_ProcessOneEventAtATime()
    {
        var options = new EventHubsOptions();

        Assert.Equal(1, options.MaxBatchSize);
        Assert.Equal(TimeSpan.FromSeconds(1), options.RetryDelay);
        Assert.Equal(TimeSpan.FromSeconds(30), options.MaxRetryDelay);
        Assert.Null(options.DeadLetterEventHub);
    }

    [Fact]
    public void ProcessorOptions_PrefetchAtLeastABatchAndLetTheAppHaveTheLastSay()
    {
        var large = EventHubsClients.CreateProcessorOptions(new EventHubsOptions { MaxBatchSize = 1000 });
        var configured = EventHubsClients.CreateProcessorOptions(new EventHubsOptions
        {
            ConfigureProcessor = o => o.LoadBalancingStrategy = LoadBalancingStrategy.Greedy,
        });

        Assert.Equal(1000, large.PrefetchCount);
        Assert.Equal(300, configured.PrefetchCount);
        Assert.Equal(LoadBalancingStrategy.Greedy, configured.LoadBalancingStrategy);
    }

    [Fact]
    public void ProducerOptions_LetTheAppHaveTheLastSay()
    {
        var options = EventHubsClients.CreateProducerOptions(new EventHubsOptions
        {
            ConfigureProducer = o => o.RetryOptions.MaximumRetries = 7,
        });

        Assert.Equal(7, options.RetryOptions.MaximumRetries);
    }

    private sealed class FakeCredential : TokenCredential
    {
        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            new("token", DateTimeOffset.MaxValue);

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            ValueTask.FromResult(GetToken(requestContext, cancellationToken));
    }
}
