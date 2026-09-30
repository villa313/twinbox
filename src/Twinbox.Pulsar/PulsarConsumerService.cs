using System.Buffers;
using DotPulsar;
using DotPulsar.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.Transport;

namespace Twinbox.Pulsar;

/// <summary>Runs <see cref="PulsarOptions.ConsumerConcurrency"/> consumers per listener, each handling one message at a time.</summary>
internal sealed partial class PulsarConsumerService(
    PulsarClients clients,
    IInboundPipeline pipeline,
    ILogger<PulsarConsumerService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRestartDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger = logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _unstarted;

    /// <summary>Completes once every consumer has connected to its subscription.</summary>
    public Task Ready => _ready.Task;

    private PulsarOptions Options => clients.Options;

    internal static TimeSpan RetryDelay(PulsarOptions options, int attempt) =>
        RetryBackoff.Delay(options.RetryDelay, options.MaxRetryDelay, attempt);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listeners = Options.Listeners;
        _unstarted = listeners.Count * Options.ConsumerConcurrency;
        if (listeners.Count == 0)
        {
            _ready.TrySetResult();
            return Task.CompletedTask;
        }

        stoppingToken.Register(() => _ready.TrySetCanceled(stoppingToken));
        var workers = listeners.SelectMany(listener => Enumerable.Range(0, Options.ConsumerConcurrency).Select(_ => RunAsync(listener, stoppingToken)));
        return Task.WhenAll(workers);
    }

    /// <summary>Keeps a consumer alive for the listener, recreating it after failures until the host stops.</summary>
    private async Task RunAsync(PulsarListener listener, CancellationToken stoppingToken)
    {
        var restartDelay = InitialRestartDelay;
        var started = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumer = clients.CreateConsumer(listener);
                await using (consumer.ConfigureAwait(false))
                {
                    await consumer.State.OnStateChangeFrom(ConsumerState.Disconnected, stoppingToken).ConfigureAwait(false);
                    LogListening(listener.Topic, listener.Subscription, Options.SubscriptionType);
                    if (!started)
                    {
                        started = true;
                        if (Interlocked.Decrement(ref _unstarted) == 0)
                        {
                            _ready.TrySetResult();
                        }
                    }

                    restartDelay = InitialRestartDelay;
                    using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    try
                    {
                        await ConsumeAsync(consumer, listener, lifetime.Token).ConfigureAwait(false);
                    }
                    finally
                    {
                        // Stops pending redeliveries before the consumer is disposed; closing it hands them back to the broker.
                        await lifetime.CancelAsync().ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                LogListenerFailed(ex, listener.Topic, listener.Subscription, restartDelay);
            }

            try
            {
                await Task.Delay(restartDelay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            restartDelay = TimeSpan.FromTicks(Math.Min(restartDelay.Ticks * 2, MaxRestartDelay.Ticks));
        }
    }

    private async Task ConsumeAsync(IConsumer<ReadOnlySequence<byte>> consumer, PulsarListener listener, CancellationToken lifetime)
    {
        // Exclusive and Failover subscriptions report every delivery as the first, so failures are tallied here instead.
        Dictionary<MessageId, int>? failures = Options.SubscriptionType is SubscriptionType.Shared or SubscriptionType.KeyShared ? null : [];
        while (true)
        {
            var message = await consumer.Receive(lifetime).ConfigureAwait(false);
            int? failed = failures is not null && failures.TryGetValue(message.MessageId, out var count) ? count : null;
            var attempt = PulsarMapping.DeliveryAttempt(message, failed);
            if (await HandleAsync(consumer, listener, message, attempt, lifetime).ConfigureAwait(false))
            {
                failures?.Remove(message.MessageId);
            }
            else if (failures is not null)
            {
                failures[message.MessageId] = attempt;
            }
        }
    }

    /// <summary>True when the message is done with (handled or dead-lettered) and was acknowledged.</summary>
    private async Task<bool> HandleAsync(
        IConsumer<ReadOnlySequence<byte>> consumer, PulsarListener listener, IMessage<ReadOnlySequence<byte>> message, int attempt, CancellationToken lifetime)
    {
        var incoming = PulsarMapping.ToIncomingMessage(listener.Topic, message, attempt);
        try
        {
            await pipeline.ProcessAsync(incoming, lifetime).ConfigureAwait(false);
        }
        catch (PermanentDeliveryException ex)
        {
            return await DeadLetterAsync(consumer, listener, message, attempt, ex, lifetime).ConfigureAwait(false);
        }
        catch (Exception) when (lifetime.IsCancellationRequested)
        {
            throw new OperationCanceledException(lifetime);
        }
        catch (Exception ex) when (attempt >= Options.MaxDeliveryAttempts)
        {
            return await DeadLetterAsync(consumer, listener, message, attempt, ex, lifetime).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var delay = RetryDelay(Options, attempt);
            LogProcessingFailed(ex, incoming.MessageId, listener.Topic, attempt, delay);
            _ = RedeliverLaterAsync(consumer, listener, message.MessageId, delay, lifetime);
            return false;
        }

        await AcknowledgeAsync(consumer, listener, message, lifetime).ConfigureAwait(false);
        return true;
    }

    private async Task<bool> DeadLetterAsync(
        IConsumer<ReadOnlySequence<byte>> consumer,
        PulsarListener listener,
        IMessage<ReadOnlySequence<byte>> message,
        int attempt,
        Exception error,
        CancellationToken lifetime)
    {
        var messageId = PulsarMapping.IdOf(listener.Topic, message);
        var deadLetterTopic = PulsarMapping.DeadLetterTopic(listener.Topic, Options.DeadLetterSuffix);
        try
        {
            var metadata = PulsarMapping.ToDeadLetter(listener.Topic, listener.Subscription, message, PulsarMapping.Describe(error));
            await clients.SendAsync(deadLetterTopic, metadata, message.Data, lifetime).ConfigureAwait(false);
        }
        catch (Exception) when (lifetime.IsCancellationRequested)
        {
            throw new OperationCanceledException(lifetime);
        }
        catch (Exception ex)
        {
            // Acknowledging without the copy would lose the message, so it is redelivered and dead-lettered again later.
            LogDeadLetterFailed(ex, messageId, listener.Topic, deadLetterTopic);
            _ = RedeliverLaterAsync(consumer, listener, message.MessageId, RetryDelay(Options, attempt), lifetime);
            return false;
        }

        LogDeadLettered(error, messageId, listener.Topic, deadLetterTopic);
        InboundDiagnostics.RecordDeadLettered(PulsarTransport.TransportName, listener.Topic);
        await AcknowledgeAsync(consumer, listener, message, lifetime).ConfigureAwait(false);
        return true;
    }

    // DotPulsar has no negative acknowledgement, so the message stays unacknowledged until the delay has passed and is then
    // handed back explicitly; the broker bumps its redelivery count, which becomes the next DeliveryAttempt.
    private async Task RedeliverLaterAsync(IConsumer consumer, PulsarListener listener, MessageId messageId, TimeSpan delay, CancellationToken lifetime)
    {
        try
        {
            await Task.Delay(delay, lifetime).ConfigureAwait(false);
            await consumer.RedeliverUnacknowledgedMessages([messageId], lifetime).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (lifetime.IsCancellationRequested)
        {
            // The consumer is closing, which returns the message to the broker anyway.
        }
        catch (Exception ex)
        {
            // It stays with this consumer until the connection drops, when the broker redelivers it.
            LogRedeliveryFailed(ex, PulsarMapping.Coordinates(listener.Topic, messageId));
        }
    }

    private async Task AcknowledgeAsync(IConsumer consumer, PulsarListener listener, IMessage message, CancellationToken lifetime)
    {
        try
        {
            await consumer.Acknowledge(message.MessageId, lifetime).ConfigureAwait(false);
        }
        catch (Exception ex) when (!lifetime.IsCancellationRequested)
        {
            // The broker redelivers it later; the inbox absorbs the repeat.
            LogAcknowledgeFailed(ex, PulsarMapping.IdOf(listener.Topic, message), listener.Topic);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming Pulsar topic {Topic} through {SubscriptionType} subscription {Subscription}.")]
    private partial void LogListening(string topic, string subscription, SubscriptionType subscriptionType);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Pulsar consumer for {Topic} ({Subscription}) failed; restarting it in {Delay}.")]
    private partial void LogListenerFailed(Exception error, string topic, string subscription, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageId} from {Topic} failed on delivery {Attempt}; redelivering it in {Delay}.")]
    private partial void LogProcessingFailed(Exception error, string messageId, string topic, int attempt, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Topic} moved to {DeadLetterTopic}.")]
    private partial void LogDeadLettered(Exception error, string messageId, string topic, string deadLetterTopic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not copy message {MessageId} from {Topic} to {DeadLetterTopic}; retrying it later.")]
    private partial void LogDeadLetterFailed(Exception error, string messageId, string topic, string deadLetterTopic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not hand message {Coordinates} back to the broker; it is redelivered once the consumer reconnects.")]
    private partial void LogRedeliveryFailed(Exception error, string coordinates);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not acknowledge message {MessageId} on {Topic}; it may be delivered again.")]
    private partial void LogAcknowledgeFailed(Exception error, string messageId, string topic);
}
