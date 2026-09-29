using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Twinbox.Transport;

namespace Twinbox.Nats;

/// <summary>Runs one durable pull consumer per listener, settling each message only after the pipeline is done with it.</summary>
internal sealed partial class NatsConsumerService(
    NatsClients clients,
    IInboundPipeline pipeline,
    ILogger<NatsConsumerService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRestartDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger = logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _unstarted;

    /// <summary>Completes once every listener's consumer exists and is being read.</summary>
    public Task Ready => _ready.Task;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listeners = clients.Options.Listeners;
        _unstarted = listeners.Count;
        if (listeners.Count == 0)
        {
            _ready.TrySetResult();
            return Task.CompletedTask;
        }

        stoppingToken.Register(() => _ready.TrySetCanceled(stoppingToken));
        return Task.WhenAll(listeners.Select(listener => RunAsync(listener, stoppingToken)));
    }

    /// <summary>Keeps a consumer running for the listener, recreating it after failures until the host stops.</summary>
    private async Task RunAsync(NatsListener listener, CancellationToken stoppingToken)
    {
        var restartDelay = InitialRestartDelay;
        var started = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await clients.EnsureStreamsAsync(stoppingToken).ConfigureAwait(false);
                var consumer = await clients.CreateConsumerAsync(listener, stoppingToken).ConfigureAwait(false);
                LogListening(listener.Stream, listener.DurableConsumer, listener.FilterSubject ?? ">");
                if (!started)
                {
                    started = true;
                    if (Interlocked.Decrement(ref _unstarted) == 0)
                    {
                        _ready.TrySetResult();
                    }
                }

                restartDelay = InitialRestartDelay;
                await ConsumeAsync(consumer, listener, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                LogListenerFailed(ex, listener.Stream, listener.DurableConsumer, restartDelay);
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

    private async Task ConsumeAsync(INatsJSConsumer consumer, NatsListener listener, CancellationToken stoppingToken)
    {
        var concurrency = clients.Options.ConsumerConcurrency;
        using var slots = new SemaphoreSlim(concurrency, concurrency);
        var consumeOptions = new NatsJSConsumeOpts { MaxMsgs = clients.Options.PrefetchCount };
        try
        {
            await foreach (var msg in consumer.ConsumeAsync(NatsRawSerializer<byte[]>.Default, consumeOptions, stoppingToken).ConfigureAwait(false))
            {
                await slots.WaitAsync(stoppingToken).ConfigureAwait(false);
                _ = HandleAndReleaseAsync(msg, listener, slots, stoppingToken);
            }
        }
        finally
        {
            // Holding every slot means no handler is still running when the consumer is replaced or the host stops.
            for (var i = 0; i < concurrency; i++)
            {
                await slots.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private async Task HandleAndReleaseAsync(NatsJSMsg<byte[]> msg, NatsListener listener, SemaphoreSlim slots, CancellationToken stoppingToken)
    {
        try
        {
            await Task.Yield();
            await HandleAsync(msg, listener, stoppingToken).ConfigureAwait(false);
        }
        finally
        {
            slots.Release();
        }
    }

    private async Task HandleAsync(NatsJSMsg<byte[]> msg, NatsListener listener, CancellationToken stoppingToken)
    {
        var attempt = (int)Math.Min(msg.Metadata?.NumDelivered ?? 1, int.MaxValue);
        var origin = NatsMapping.Origin(msg.Metadata?.Stream ?? listener.Stream, msg.Metadata?.Sequence.Stream ?? 0);
        var message = NatsMapping.ToIncomingMessage(msg.Subject, msg.Data ?? [], msg.Headers, attempt, origin);
        try
        {
            await pipeline.ProcessAsync(message, stoppingToken).ConfigureAwait(false);
            await SettleAsync(msg, message, m => m.AckAsync(cancellationToken: CancellationToken.None)).ConfigureAwait(false);
        }
        catch (PermanentDeliveryException ex)
        {
            await DeadLetterAsync(msg, message, origin, ex, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            // Handing it back now lets another instance take it without waiting out AckWait.
            await SettleAsync(msg, message, m => m.NakAsync(cancellationToken: CancellationToken.None)).ConfigureAwait(false);
        }
        catch (Exception ex) when (attempt >= clients.Options.MaxDeliver)
        {
            LogDeliveriesExhausted(ex, message.MessageId, message.Source, attempt);
            await DeadLetterAsync(msg, message, origin, ex, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            var delay = NatsMapping.RetryDelay(clients.Options, attempt);
            LogProcessingFailed(ex, message.MessageId, message.Source, attempt, delay);
            await SettleAsync(msg, message, m => m.NakAsync(delay: delay, cancellationToken: CancellationToken.None)).ConfigureAwait(false);
        }
    }

    private async Task DeadLetterAsync(NatsJSMsg<byte[]> msg, IncomingMessage message, string origin, Exception error, CancellationToken stoppingToken)
    {
        if (clients.Options.DeadLetterSubject is not { } deadLetterSubject)
        {
            LogTerminated(error, message.MessageId, message.Source);
            await SettleAsync(msg, message, m => m.AckTerminateAsync(cancellationToken: CancellationToken.None)).ConfigureAwait(false);
            return;
        }

        try
        {
            var ack = await clients.PublishAsync(deadLetterSubject, message.Body, NatsMapping.ToDeadLetterHeaders(message, origin, error), stoppingToken)
                .ConfigureAwait(false);
            if (ack.Error is { } rejection)
            {
                throw new NatsJSApiException(rejection);
            }
        }
        catch (Exception ex)
        {
            // Terminating without the copy would lose the message, so it goes back for another attempt.
            var delay = NatsMapping.RetryDelay(clients.Options, message.DeliveryAttempt);
            LogDeadLetterFailed(ex, message.MessageId, message.Source, deadLetterSubject);
            await SettleAsync(msg, message, m => m.NakAsync(delay: delay, cancellationToken: CancellationToken.None)).ConfigureAwait(false);
            return;
        }

        LogDeadLettered(error, message.MessageId, message.Source, deadLetterSubject);
        await SettleAsync(msg, message, m => m.AckTerminateAsync(cancellationToken: CancellationToken.None)).ConfigureAwait(false);
    }

    private async Task SettleAsync(NatsJSMsg<byte[]> msg, IncomingMessage message, Func<NatsJSMsg<byte[]>, ValueTask> settle)
    {
        try
        {
            await settle(msg).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The server redelivers once AckWait runs out, and the inbox absorbs the repeat.
            LogSettleFailed(ex, message.MessageId, message.Source);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming NATS stream {Stream} as durable consumer {Consumer} filtered to '{FilterSubject}'.")]
    private partial void LogListening(string stream, string consumer, string filterSubject);

    [LoggerMessage(Level = LogLevel.Warning, Message = "NATS consumer {Consumer} on stream {Stream} failed; restarting it in {Delay}.")]
    private partial void LogListenerFailed(Exception error, string stream, string consumer, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageId} from {Subject} failed on attempt {Attempt}; redelivering it in {Delay}.")]
    private partial void LogProcessingFailed(Exception error, string messageId, string subject, int attempt, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Subject} failed on its last allowed delivery ({Attempt}); dead-lettering it.")]
    private partial void LogDeliveriesExhausted(Exception error, string messageId, string subject, int attempt);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Subject} failed permanently; copied it to {DeadLetterSubject} and terminated it.")]
    private partial void LogDeadLettered(Exception error, string messageId, string subject, string deadLetterSubject);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Subject} failed permanently and no dead-letter subject is set; terminating it.")]
    private partial void LogTerminated(Exception error, string messageId, string subject);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not copy message {MessageId} from {Subject} to {DeadLetterSubject}; redelivering it.")]
    private partial void LogDeadLetterFailed(Exception error, string messageId, string subject, string deadLetterSubject);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not settle message {MessageId} from {Subject}; the server will redeliver it after AckWait.")]
    private partial void LogSettleFailed(Exception error, string messageId, string subject);
}
