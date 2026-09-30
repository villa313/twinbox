using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Testcontainers.PubSub;

namespace Twinbox.GooglePubSub.Tests;

public sealed class PubSubFixture : IAsyncLifetime
{
    public const string ProjectId = "twinbox-test";

    private readonly PubSubContainer _container = new PubSubBuilder("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators").Build();

    private PublisherServiceApiClient? _publisher;
    private SubscriberServiceApiClient? _subscriber;

    /// <summary>host:port, the form the client libraries expect for an emulator.</summary>
    public string EmulatorHost
    {
        get
        {
            var endpoint = _container.GetEmulatorEndpoint();
            return endpoint.Contains("://", StringComparison.Ordinal) ? new Uri(endpoint).Authority : endpoint;
        }
    }

    public PublisherServiceApiClient Publisher => _publisher ??= new PublisherServiceApiClientBuilder
    {
        Endpoint = EmulatorHost,
        ChannelCredentials = ChannelCredentials.Insecure,
    }.Build();

    public SubscriberServiceApiClient Subscriber => _subscriber ??= new SubscriberServiceApiClientBuilder
    {
        Endpoint = EmulatorHost,
        ChannelCredentials = ChannelCredentials.Insecure,
    }.Build();

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public ValueTask DisposeAsync() => _container.DisposeAsync();

    public async Task CreateTopicAsync(string topic) => await Publisher.CreateTopicAsync(new TopicName(ProjectId, topic));

    public async Task CreateSubscriptionAsync(string subscription, string topic) =>
        await Subscriber.CreateSubscriptionAsync(new SubscriptionName(ProjectId, subscription), new TopicName(ProjectId, topic), pushConfig: null, ackDeadlineSeconds: 10);

    /// <summary>Pulls and acknowledges the next message, for inspecting what reached a subscription nobody consumes.</summary>
    public async Task<PubsubMessage> PullOneAsync(string subscription, TimeSpan timeout)
    {
        var name = new SubscriptionName(ProjectId, subscription);
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            var response = await Subscriber.PullAsync(new PullRequest { SubscriptionAsSubscriptionName = name, MaxMessages = 1 }, cts.Token);
            if (response.ReceivedMessages is [var received, ..])
            {
                await Subscriber.AcknowledgeAsync(name, [received.AckId], cts.Token);
                return received.Message;
            }

            await Task.Delay(100, cts.Token);
        }
    }
}
