using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.Transport;

namespace Twinbox.AmazonSqs;

/// <summary>Long-polls each listened queue, handling up to <see cref="AmazonSqsOptions.MaxConcurrency"/> messages per queue at once.</summary>
internal sealed partial class AmazonSqsReceiverService(
    AmazonSqsClients clients,
    IInboundPipeline pipeline,
    ILogger<AmazonSqsReceiverService> logger) : BackgroundService
{
    private const int MaxVisibilitySeconds = 43_200;

    private static readonly TimeSpan InitialRestartDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger = logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _unprepared;

    /// <summary>Completes once every listened queue (and its subscriptions, when auto-created) is ready.</summary>
    public Task Ready => _ready.Task;

    /// <summary>Doubles <see cref="AmazonSqsOptions.RetryDelay"/> per failed attempt, capped at <see cref="AmazonSqsOptions.MaxRetryDelay"/>.</summary>
    internal static TimeSpan RetryDelay(AmazonSqsOptions options, int attempt)
    {
        var factor = Math.Pow(2, Math.Clamp(attempt - 1, 0, 30));
        var ticks = Math.Min(options.RetryDelay.Ticks * factor, options.MaxRetryDelay.Ticks);
        return TimeSpan.FromTicks((long)ticks);
    }

    /// <summary>Visibility timeouts are whole seconds, so sub-second delays round up rather than retry immediately.</summary>
    internal static int ToVisibilitySeconds(TimeSpan delay) => (int)Math.Clamp(Math.Ceiling(delay.TotalSeconds), 0, MaxVisibilitySeconds);

    /// <summary>Units are handled sequentially: a standard queue's messages one each, a FIFO group's together in receive order.</summary>
    internal static List<List<Message>> Partition(IReadOnlyList<Message> messages, bool fifo)
    {
        if (!fifo)
        {
            return [.. messages.Select(m => new List<Message> { m })];
        }

        return [.. messages
            .GroupBy(m => m.Attributes?.GetValueOrDefault(AmazonSqsMapping.GroupIdAttribute) ?? string.Empty, StringComparer.Ordinal)
            .Select(g => g.ToList())];
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queues = clients.Options.Listeners
            .GroupBy(l => l.Queue, StringComparer.Ordinal)
            .Select(g => new QueueListener(g.Key, [.. g.Select(l => l.Topic).OfType<string>().Distinct(StringComparer.Ordinal)]))
            .ToList();
        _unprepared = queues.Count;
        if (queues.Count == 0)
        {
            _ready.TrySetResult();
            return Task.CompletedTask;
        }

        stoppingToken.Register(() => _ready.TrySetCanceled(stoppingToken));
        return Task.WhenAll(queues.Select(queue => RunAsync(queue, stoppingToken)));
    }

    /// <summary>Keeps polling the queue until the host stops, backing off and re-resolving it after failures.</summary>
    private async Task RunAsync(QueueListener listener, CancellationToken stoppingToken)
    {
        var options = clients.Options;
        using var slots = new SemaphoreSlim(options.MaxConcurrency, options.MaxConcurrency);
        var fifo = AmazonSqsMapping.IsFifo(listener.Queue);
        var restartDelay = InitialRestartDelay;
        var prepared = false;
        string? url = null;
        try
        {
            while (true)
            {
                try
                {
                    url ??= await PrepareAsync(listener, stoppingToken).ConfigureAwait(false);
                    if (!prepared)
                    {
                        prepared = true;
                        LogListening(listener.Queue, url);
                        if (Interlocked.Decrement(ref _unprepared) == 0)
                        {
                            _ready.TrySetResult();
                        }
                    }

                    await PollAsync(listener.Queue, url, fifo, slots, stoppingToken).ConfigureAwait(false);
                    restartDelay = InitialRestartDelay;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    if (AmazonSqsErrors.IsMissingQueue(ex))
                    {
                        clients.ForgetQueue(listener.Queue);
                        url = null;
                    }

                    LogPollFailed(ex, listener.Queue, restartDelay);
                    await Task.Delay(restartDelay, stoppingToken).ConfigureAwait(false);
                    restartDelay = TimeSpan.FromTicks(Math.Min(restartDelay.Ticks * 2, MaxRestartDelay.Ticks));
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Stopping while backing off.
        }
        finally
        {
            // Let in-flight handlers settle their messages before the clients are disposed.
            for (var i = 0; i < options.MaxConcurrency; i++)
            {
                await slots.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private async Task<string> PrepareAsync(QueueListener listener, CancellationToken stoppingToken)
    {
        var url = await clients.GetQueueUrlAsync(listener.Queue, stoppingToken).ConfigureAwait(false);
        if (clients.Options.AutoCreate)
        {
            foreach (var topic in listener.Topics)
            {
                await clients.SubscribeAsync(url, topic, stoppingToken).ConfigureAwait(false);
            }

            if (clients.Options.DeadLetterQueue is { } deadLetterQueue)
            {
                await clients.GetQueueUrlAsync(deadLetterQueue, stoppingToken).ConfigureAwait(false);
            }
        }

        return url;
    }

    /// <summary>Receives as many messages as there are free slots and hands each unit to its own task.</summary>
    private async Task PollAsync(string queue, string url, bool fifo, SemaphoreSlim slots, CancellationToken stoppingToken)
    {
        await slots.WaitAsync(stoppingToken).ConfigureAwait(false);
        var taken = 1;
        while (taken < clients.Options.MaxNumberOfMessages && slots.Wait(0, CancellationToken.None))
        {
            taken++;
        }

        List<Message> messages;
        try
        {
            var response = await clients.Sqs.ReceiveMessageAsync(
                new ReceiveMessageRequest
                {
                    QueueUrl = url,
                    MaxNumberOfMessages = taken,
                    WaitTimeSeconds = clients.Options.WaitTimeSeconds,
                    VisibilityTimeout = ToVisibilitySeconds(clients.Options.VisibilityTimeout),
                    MessageAttributeNames = ["All"],
                    MessageSystemAttributeNames = [AmazonSqsMapping.ReceiveCountAttribute, AmazonSqsMapping.GroupIdAttribute],
                },
                stoppingToken).ConfigureAwait(false);
            messages = response.Messages ?? [];
        }
        catch
        {
            slots.Release(taken);
            throw;
        }

        if (messages.Count < taken)
        {
            slots.Release(taken - messages.Count);
        }

        foreach (var unit in Partition(messages, fifo))
        {
            _ = Task.Run(() => HandleUnitAsync(queue, url, unit, slots, stoppingToken), CancellationToken.None);
        }
    }

    /// <summary>Once a message is left unsettled, the rest come back with it, so a FIFO group is retried in its original order.</summary>
    private async Task HandleUnitAsync(string queue, string url, List<Message> unit, SemaphoreSlim slots, CancellationToken stoppingToken)
    {
        TimeSpan? heldFor = null;
        foreach (var message in unit)
        {
            try
            {
                if (heldFor is { } delay)
                {
                    await SetVisibilityAsync(queue, url, message, delay).ConfigureAwait(false);
                }
                else
                {
                    heldFor = await HandleAsync(queue, url, message, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                LogHandlingFailed(ex, message.MessageId, queue);
                heldFor ??= clients.Options.VisibilityTimeout;
            }
            finally
            {
                slots.Release();
            }
        }
    }

    /// <summary>Null once the message is deleted; otherwise how long until it becomes visible again.</summary>
    private async Task<TimeSpan?> HandleAsync(string queue, string url, Message message, CancellationToken stoppingToken)
    {
        var incoming = AmazonSqsMapping.ToIncoming(message, queue);
        try
        {
            await pipeline.ProcessAsync(incoming, stoppingToken).ConfigureAwait(false);
        }
        catch (PermanentDeliveryException ex)
        {
            return await DeadLetterAsync(queue, url, message, incoming, ex, stoppingToken).ConfigureAwait(false);
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            // Hand it straight back so another instance can pick it up without waiting out the visibility timeout.
            await SetVisibilityAsync(queue, url, message, TimeSpan.Zero).ConfigureAwait(false);
            return TimeSpan.Zero;
        }
        catch (Exception ex)
        {
            var delay = RetryDelay(clients.Options, incoming.DeliveryAttempt);
            LogProcessingFailed(ex, incoming.MessageId, queue, incoming.DeliveryAttempt, delay);
            await SetVisibilityAsync(queue, url, message, delay).ConfigureAwait(false);
            return delay;
        }

        return await DeleteAsync(queue, url, message).ConfigureAwait(false);
    }

    private async Task<TimeSpan?> DeadLetterAsync(
        string queue,
        string url,
        Message message,
        IncomingMessage incoming,
        PermanentDeliveryException error,
        CancellationToken stoppingToken)
    {
        if (clients.Options.DeadLetterQueue is not { } deadLetterQueue)
        {
            LogLeftForRedrive(error, incoming.MessageId, queue);
            return clients.Options.VisibilityTimeout;
        }

        try
        {
            await clients.SendToQueueAsync(AmazonSqsMapping.ToDeadLetter(incoming, error, deadLetterQueue), stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Deleting without the copy would lose the message, so it is retried like a handler failure.
            var delay = RetryDelay(clients.Options, incoming.DeliveryAttempt);
            LogDeadLetterFailed(ex, incoming.MessageId, queue, deadLetterQueue);
            await SetVisibilityAsync(queue, url, message, delay).ConfigureAwait(false);
            return delay;
        }

        LogDeadLettered(error, incoming.MessageId, queue, deadLetterQueue);
        return await DeleteAsync(queue, url, message).ConfigureAwait(false);
    }

    private async Task<TimeSpan?> DeleteAsync(string queue, string url, Message message)
    {
        try
        {
            await clients.Sqs.DeleteMessageAsync(
                new DeleteMessageRequest { QueueUrl = url, ReceiptHandle = message.ReceiptHandle },
                CancellationToken.None).ConfigureAwait(false);
            return null;
        }
        catch (Exception ex)
        {
            // It reappears after the visibility timeout and the inbox absorbs the repeat.
            LogDeleteFailed(ex, message.MessageId, queue);
            return clients.Options.VisibilityTimeout;
        }
    }

    private async Task SetVisibilityAsync(string queue, string url, Message message, TimeSpan delay)
    {
        try
        {
            await clients.Sqs.ChangeMessageVisibilityAsync(
                new ChangeMessageVisibilityRequest
                {
                    QueueUrl = url,
                    ReceiptHandle = message.ReceiptHandle,
                    VisibilityTimeout = ToVisibilitySeconds(delay),
                },
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The original visibility timeout still applies, so the message comes back regardless.
            LogVisibilityFailed(ex, message.MessageId, queue);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Receiving from SQS queue {Queue} ({QueueUrl}).")]
    private partial void LogListening(string queue, string queueUrl);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Receiving from SQS queue {Queue} failed; retrying in {Delay}.")]
    private partial void LogPollFailed(Exception error, string queue, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageId} from {Queue} failed on attempt {Attempt}; retrying it in {Delay}.")]
    private partial void LogProcessingFailed(Exception error, string messageId, string queue, int attempt, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Queue} failed permanently; copied it to {DeadLetterQueue}.")]
    private partial void LogDeadLettered(Exception error, string messageId, string queue, string deadLetterQueue);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Queue} failed permanently and no dead-letter queue is set; leaving it to the queue's redrive policy.")]
    private partial void LogLeftForRedrive(Exception error, string messageId, string queue);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not copy message {MessageId} from {Queue} to {DeadLetterQueue}; retrying it.")]
    private partial void LogDeadLetterFailed(Exception error, string messageId, string queue, string deadLetterQueue);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not delete message {MessageId} from {Queue}; it will be redelivered.")]
    private partial void LogDeleteFailed(Exception error, string messageId, string queue);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Could not change the visibility of message {MessageId} in {Queue}.")]
    private partial void LogVisibilityFailed(Exception error, string messageId, string queue);

    [LoggerMessage(Level = LogLevel.Error, Message = "Handling message {MessageId} from {Queue} failed unexpectedly.")]
    private partial void LogHandlingFailed(Exception error, string messageId, string queue);

    private sealed record QueueListener(string Queue, string[] Topics);
}
