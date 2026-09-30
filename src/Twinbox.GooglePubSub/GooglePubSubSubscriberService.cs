using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.Transport;

namespace Twinbox.GooglePubSub;

/// <summary>Runs a subscriber per subscription, acknowledging each message only once the inbox has handled it.</summary>
internal sealed partial class GooglePubSubSubscriberService(
    GooglePubSubClients clients,
    IInboundPipeline pipeline,
    ILogger<GooglePubSubSubscriberService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRestartDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);

    // The longest backoff a subscription retry policy allows.
    private static readonly TimeSpan MaxRedeliveryDelay = TimeSpan.FromSeconds(600);

    private readonly ILogger _logger = logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _unprepared;

    /// <summary>Completes once every subscription (and its topics, when auto-created) is ready and being pulled.</summary>
    public Task Ready => _ready.Task;

    internal async Task<SubscriberClient.Reply> HandleAsync(
        string subscription,
        PubsubMessage message,
        OrderingKeyGate gate,
        CancellationToken stoppingToken)
    {
        var orderingKey = message.OrderingKey;
        var publishTime = message.PublishTime?.ToDateTimeOffset() ?? default;
        if (orderingKey.Length > 0 && !gate.TryEnter(orderingKey, message.MessageId, publishTime))
        {
            return SubscriberClient.Reply.Nack;
        }

        var reply = await HandleCoreAsync(subscription, message, stoppingToken).ConfigureAwait(false);
        if (reply == SubscriberClient.Reply.Nack && orderingKey.Length > 0)
        {
            gate.Fail(orderingKey, message.MessageId, publishTime);
        }

        return reply;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listeners = clients.Options.Subscriptions
            .DistinctBy(s => s.Subscription, StringComparer.Ordinal)
            .ToList();
        _unprepared = listeners.Count;
        if (listeners.Count == 0)
        {
            _ready.TrySetResult();
            return Task.CompletedTask;
        }

        stoppingToken.Register(() => _ready.TrySetCanceled(stoppingToken));
        return Task.WhenAll(listeners.Select(listener => RunAsync(listener, stoppingToken)));
    }

    /// <summary>Keeps a subscriber running until the host stops, backing off and rebuilding it after failures.</summary>
    private async Task RunAsync(GooglePubSubSubscription listener, CancellationToken stoppingToken)
    {
        var name = GooglePubSubMapping.ToSubscriptionName(listener.Subscription, clients.Options.ProjectId);
        var restartDelay = InitialRestartDelay;
        var prepared = false;
        var ensured = !clients.Options.AutoCreate;
        try
        {
            while (true)
            {
                var started = Stopwatch.GetTimestamp();
                try
                {
                    if (!ensured)
                    {
                        await clients.EnsureSubscriptionAsync(listener, stoppingToken).ConfigureAwait(false);
                        ensured = true;
                    }

                    var subscriber = await clients.CreateSubscriberAsync(name, stoppingToken).ConfigureAwait(false);
                    var gate = new OrderingKeyGate(MaxRedeliveryDelay + clients.Options.AckDeadline, TimeProvider.System);
                    var running = subscriber.StartAsync((message, _) => HandleAsync(listener.Subscription, message, gate, stoppingToken));
                    if (!prepared)
                    {
                        prepared = true;
                        LogListening(name.ToString());
                        if (Interlocked.Decrement(ref _unprepared) == 0)
                        {
                            _ready.TrySetResult();
                        }
                    }

                    await RunUntilStoppedAsync(subscriber, running, stoppingToken).ConfigureAwait(false);
                    if (stoppingToken.IsCancellationRequested)
                    {
                        return;
                    }

                    throw new InvalidOperationException($"The subscriber for {name} stopped unexpectedly.");
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // Recreate a subscription deleted while running, as it was created at startup.
                    ensured &= !(clients.Options.AutoCreate && GooglePubSubErrors.IsNotFound(ex));

                    // A subscriber that ran for a while failed afresh, so it starts over from the shortest delay.
                    if (Stopwatch.GetElapsedTime(started) > MaxRestartDelay)
                    {
                        restartDelay = InitialRestartDelay;
                    }

                    LogSubscriberFailed(ex, name.ToString(), restartDelay);
                    await Task.Delay(restartDelay, stoppingToken).ConfigureAwait(false);
                    restartDelay = TimeSpan.FromTicks(Math.Min(restartDelay.Ticks * 2, MaxRestartDelay.Ticks));
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Stopping while backing off.
        }
    }

    /// <summary>Returns once the host stops, after in-flight handlers have settled; throws if the subscriber fails first.</summary>
    private static async Task RunUntilStoppedAsync(SubscriberClient subscriber, Task running, CancellationToken stoppingToken)
    {
        try
        {
            await running.WaitAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Handlers see the same token and nack promptly, so this also flushes their acks before the host moves on.
            await subscriber.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task<SubscriberClient.Reply> HandleCoreAsync(string subscription, PubsubMessage message, CancellationToken stoppingToken)
    {
        IncomingMessage incoming;
        try
        {
            incoming = GooglePubSubMapping.ToIncoming(message, subscription);
        }
        catch (Exception ex)
        {
            LogHandlingFailed(ex, message.MessageId, subscription);
            return SubscriberClient.Reply.Nack;
        }

        try
        {
            await pipeline.ProcessAsync(incoming, stoppingToken).ConfigureAwait(false);
            return SubscriberClient.Reply.Ack;
        }
        catch (PermanentDeliveryException ex)
        {
            return await DeadLetterAsync(subscription, message, incoming, ex).ConfigureAwait(false);
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            // Hand it straight back so another instance can pick it up without waiting out the ack deadline.
            return SubscriberClient.Reply.Nack;
        }
        catch (Exception ex)
        {
            LogProcessingFailed(ex, incoming.MessageId, subscription, incoming.DeliveryAttempt);
            return SubscriberClient.Reply.Nack;
        }
    }

    private async Task<SubscriberClient.Reply> DeadLetterAsync(
        string subscription,
        PubsubMessage message,
        IncomingMessage incoming,
        PermanentDeliveryException error)
    {
        if (clients.Options.DeadLetterTopic is not { } deadLetterTopic)
        {
            // Pub/Sub reports a delivery attempt only to subscriptions with a dead-letter policy, which can forward it.
            if (message.GetDeliveryAttempt() is not null)
            {
                LogLeftForDeadLetterPolicy(error, incoming.MessageId, subscription);
                return SubscriberClient.Reply.Nack;
            }

            LogDiscarded(error, incoming.MessageId, subscription, incoming.MessageName);
            InboundDiagnostics.RecordDiscarded(GooglePubSubTransport.TransportName, subscription);
            return SubscriberClient.Reply.Ack;
        }

        try
        {
            var topic = GooglePubSubMapping.ToTopicName(deadLetterTopic, clients.Options.ProjectId);
            await clients.PublishAsync(topic, GooglePubSubMapping.ToDeadLetter(message, incoming, error), CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Acknowledging without the copy would lose the message, so it is redelivered like a handler failure.
            LogDeadLetterFailed(ex, incoming.MessageId, subscription, deadLetterTopic);
            return SubscriberClient.Reply.Nack;
        }

        LogDeadLettered(error, incoming.MessageId, subscription, deadLetterTopic);
        InboundDiagnostics.RecordDeadLettered(GooglePubSubTransport.TransportName, subscription);
        return SubscriberClient.Reply.Ack;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Receiving from Pub/Sub subscription {Subscription}.")]
    private partial void LogListening(string subscription);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The Pub/Sub subscriber for {Subscription} failed; restarting it in {Delay}.")]
    private partial void LogSubscriberFailed(Exception error, string subscription, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageId} from {Subscription} failed on attempt {Attempt}; Pub/Sub will redeliver it.")]
    private partial void LogProcessingFailed(Exception error, string messageId, string subscription, int attempt);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Subscription} failed permanently; copied it to {DeadLetterTopic}.")]
    private partial void LogDeadLettered(Exception error, string messageId, string subscription, string deadLetterTopic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Subscription} failed permanently and no dead-letter topic is set; leaving it to the subscription's dead-letter policy.")]
    private partial void LogLeftForDeadLetterPolicy(Exception error, string messageId, string subscription);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} ('{MessageName}') from {Subscription} failed permanently and neither a dead-letter topic nor a dead-letter policy is set; acknowledging it.")]
    private partial void LogDiscarded(Exception error, string messageId, string subscription, string messageName);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not copy message {MessageId} from {Subscription} to {DeadLetterTopic}; Pub/Sub will redeliver it.")]
    private partial void LogDeadLetterFailed(Exception error, string messageId, string subscription, string deadLetterTopic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Handling message {MessageId} from {Subscription} failed unexpectedly.")]
    private partial void LogHandlingFailed(Exception error, string messageId, string subscription);
}
