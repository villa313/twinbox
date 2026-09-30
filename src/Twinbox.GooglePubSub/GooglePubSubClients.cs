using System.Collections.Concurrent;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Twinbox.GooglePubSub;

/// <summary>Owns one publisher per topic, shared by sending and dead-lettering, and creates topics and subscriptions when asked to.</summary>
internal sealed partial class GooglePubSubClients(IOptions<GooglePubSubOptions> options, ILogger<GooglePubSubClients> logger) : IAsyncDisposable, IDisposable
{
    private static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(10);

    private readonly ILogger _logger = logger;
    private readonly ConcurrentDictionary<string, Lazy<Task<PublisherClient>>> _publishers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _ensuredTopics = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _adminGate = new(1, 1);
    private PublisherServiceApiClient? _topicAdmin;
    private SubscriberServiceApiClient? _subscriptionAdmin;
    private volatile bool _disposed;

    public GooglePubSubOptions Options { get; } = options.Value;

    public async Task PublishAsync(TopicName topic, PubsubMessage message, CancellationToken cancellationToken)
    {
        if (Options.AutoCreate)
        {
            await EnsureTopicAsync(topic, cancellationToken).ConfigureAwait(false);
        }

        var publisher = await GetPublisherAsync(topic, cancellationToken).ConfigureAwait(false);
        try
        {
            await publisher.PublishAsync(message).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (message.OrderingKey.Length > 0)
            {
                // A failed publish pauses its ordering key, and the outbox will retry this very message.
                publisher.ResumePublish(message.OrderingKey);
            }

            if (GooglePubSubErrors.IsNotFound(ex))
            {
                _ensuredTopics.TryRemove(topic.ToString(), out _);
            }

            throw;
        }
    }

    public async Task EnsureTopicAsync(TopicName topic, CancellationToken cancellationToken)
    {
        var key = topic.ToString();
        if (_ensuredTopics.ContainsKey(key))
        {
            return;
        }

        var admin = await GetTopicAdminAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await admin.CreateTopicAsync(topic, cancellationToken).ConfigureAwait(false);
            LogTopicCreated(key);
        }
        catch (Exception ex) when (GooglePubSubErrors.IsAlreadyExists(ex))
        {
            // Created elsewhere, which is just as good.
        }

        _ensuredTopics[key] = true;
    }

    /// <summary>Creates the subscription, its topic and the dead-letter topic; an existing subscription is left unchanged.</summary>
    public async Task EnsureSubscriptionAsync(GooglePubSubSubscription listener, CancellationToken cancellationToken)
    {
        var topic = GooglePubSubMapping.ToTopicName(listener.Topic, Options.ProjectId);
        await EnsureTopicAsync(topic, cancellationToken).ConfigureAwait(false);

        var subscription = new Subscription
        {
            SubscriptionName = GooglePubSubMapping.ToSubscriptionName(listener.Subscription, Options.ProjectId),
            TopicAsTopicName = topic,
            AckDeadlineSeconds = (int)Options.AckDeadline.TotalSeconds,
            EnableMessageOrdering = Options.EnableMessageOrdering,
        };
        if (Options.DeadLetterTopic is { } deadLetterTopic)
        {
            var deadLetterTopicName = GooglePubSubMapping.ToTopicName(deadLetterTopic, Options.ProjectId);
            await EnsureTopicAsync(deadLetterTopicName, cancellationToken).ConfigureAwait(false);
            subscription.DeadLetterPolicy = new DeadLetterPolicy
            {
                DeadLetterTopic = deadLetterTopicName.ToString(),
                MaxDeliveryAttempts = Options.MaxDeliveryAttempts,
            };
        }

        var admin = await GetSubscriptionAdminAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await admin.CreateSubscriptionAsync(subscription, cancellationToken).ConfigureAwait(false);
            LogSubscriptionCreated(subscription.Name, subscription.Topic);
        }
        catch (Exception ex) when (GooglePubSubErrors.IsAlreadyExists(ex))
        {
            // Its settings are the operator's to change, not ours.
        }
    }

    public Task<SubscriberClient> CreateSubscriberAsync(SubscriptionName subscription, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var builder = new SubscriberClientBuilder
        {
            SubscriptionName = subscription,
            EmulatorDetection = EmulatorMode,
            Settings = new SubscriberClient.Settings
            {
                AckDeadline = Options.AckDeadline,
                FlowControlSettings = new FlowControlSettings(Options.MaxOutstandingMessages, null),
            },
        };
        Connect(builder);
        Options.ConfigureSubscriber?.Invoke(builder);
        return builder.BuildAsync(cancellationToken);
    }

    /// <summary>Lets a service provider that is disposed synchronously still flush the publishers.</summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (var pending in _publishers.Values)
        {
            if (!pending.IsValueCreated || !pending.Value.IsCompletedSuccessfully)
            {
                continue;
            }

            try
            {
                await pending.Value.Result.ShutdownAsync(ShutdownTimeout).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogShutdownFailed(ex, pending.Value.Result.TopicName.ToString());
            }
        }

        _publishers.Clear();
        _adminGate.Dispose();
    }

    /// <summary>Other Google client libraries honour PUBSUB_EMULATOR_HOST by default, so these do too unless a host is given.</summary>
    internal EmulatorDetection EmulatorMode =>
        Options.EmulatorHost is null ? EmulatorDetection.EmulatorOrProduction : EmulatorDetection.None;

    internal void Connect<TClient>(ClientBuilderBase<TClient> builder)
    {
        if (Options.EmulatorHost is { } emulatorHost)
        {
            builder.Endpoint = emulatorHost;
            builder.ChannelCredentials = ChannelCredentials.Insecure;
        }
        else
        {
            builder.GoogleCredential = Options.Credential;
        }
    }

    private async Task<PublisherClient> GetPublisherAsync(TopicName topic, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var key = topic.ToString();
        var pending = _publishers.GetOrAdd(key, _ => new Lazy<Task<PublisherClient>>(() => CreatePublisherAsync(topic)));
        try
        {
            return await pending.Value.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (pending.Value.IsFaulted || pending.Value.IsCanceled)
        {
            // Don't cache a failed creation, e.g. credentials that were not yet available.
            _publishers.TryRemove(new KeyValuePair<string, Lazy<Task<PublisherClient>>>(key, pending));
            throw;
        }
    }

    private Task<PublisherClient> CreatePublisherAsync(TopicName topic)
    {
        var builder = new PublisherClientBuilder
        {
            TopicName = topic,
            EmulatorDetection = EmulatorMode,
            Settings = new PublisherClient.Settings { EnableMessageOrdering = Options.EnableMessageOrdering },
        };
        Connect(builder);
        Options.ConfigurePublisher?.Invoke(builder);
        return builder.BuildAsync(CancellationToken.None);
    }

    private async Task<PublisherServiceApiClient> GetTopicAdminAsync(CancellationToken cancellationToken)
    {
        if (_topicAdmin is { } admin)
        {
            return admin;
        }

        await _adminGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_topicAdmin is null)
            {
                var builder = new PublisherServiceApiClientBuilder { EmulatorDetection = EmulatorMode };
                Connect(builder);
                _topicAdmin = await builder.BuildAsync(cancellationToken).ConfigureAwait(false);
            }

            return _topicAdmin;
        }
        finally
        {
            _adminGate.Release();
        }
    }

    private async Task<SubscriberServiceApiClient> GetSubscriptionAdminAsync(CancellationToken cancellationToken)
    {
        if (_subscriptionAdmin is { } admin)
        {
            return admin;
        }

        await _adminGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_subscriptionAdmin is null)
            {
                var builder = new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorMode };
                Connect(builder);
                _subscriptionAdmin = await builder.BuildAsync(cancellationToken).ConfigureAwait(false);
            }

            return _subscriptionAdmin;
        }
        finally
        {
            _adminGate.Release();
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created Pub/Sub topic {Topic}.")]
    private partial void LogTopicCreated(string topic);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created Pub/Sub subscription {Subscription} to {Topic}.")]
    private partial void LogSubscriptionCreated(string subscription, string topic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The Pub/Sub publisher for {Topic} did not shut down cleanly; unsent messages stay in the outbox.")]
    private partial void LogShutdownFailed(Exception error, string topic);
}
