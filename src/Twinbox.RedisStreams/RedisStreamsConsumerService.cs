using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Twinbox.Transport;

namespace Twinbox.RedisStreams;

/// <summary>Reads each listener's stream as a consumer group member, acknowledging entries only once they are done with.</summary>
internal sealed partial class RedisStreamsConsumerService(
    RedisStreamsConnection connection,
    IInboundPipeline pipeline,
    ILogger<RedisStreamsConsumerService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRestartDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);
    private static readonly RedisValue StreamStart = "0-0";

    private readonly ILogger _logger = logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _unstarted;

    /// <summary>Completes once every listener's consumer group exists.</summary>
    public Task Ready => _ready.Task;

    private RedisStreamsOptions Options => connection.Options;

    /// <summary>Half the claim threshold, so an abandoned entry is picked up within 1.5x <see cref="RedisStreamsOptions.ClaimIdleAfter"/>.</summary>
    internal static TimeSpan SweepInterval(RedisStreamsOptions options) => options.ClaimIdleAfter / 2;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listeners = Options.Listeners;
        _unstarted = listeners.Count;
        if (listeners.Count == 0)
        {
            _ready.TrySetResult();
            return Task.CompletedTask;
        }

        stoppingToken.Register(() => _ready.TrySetCanceled(stoppingToken));
        return Task.WhenAll(listeners.Select(listener => RunAsync(listener, stoppingToken)));
    }

    /// <summary>Keeps the listener consuming, starting over after failures until the host stops.</summary>
    private async Task RunAsync(RedisStreamsListener listener, CancellationToken stoppingToken)
    {
        var restartDelay = InitialRestartDelay;
        var started = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var database = await connection.GetDatabaseAsync(stoppingToken).ConfigureAwait(false);
                await EnsureGroupAsync(database, listener, stoppingToken).ConfigureAwait(false);
                LogListening(listener.Stream, listener.Group, Options.ConsumerName);
                if (!started)
                {
                    started = true;
                    if (Interlocked.Decrement(ref _unstarted) == 0)
                    {
                        _ready.TrySetResult();
                    }
                }

                restartDelay = InitialRestartDelay;
                await ConsumeAsync(database, listener, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                LogListenerFailed(ex, listener.Stream, listener.Group, restartDelay);
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

    private async Task EnsureGroupAsync(IDatabase database, RedisStreamsListener listener, CancellationToken stoppingToken)
    {
        try
        {
            await database.StreamCreateConsumerGroupAsync(listener.Stream, listener.Group, StreamStart, createStream: true)
                .WaitAsync(stoppingToken).ConfigureAwait(false);
            LogGroupCreated(listener.Stream, listener.Group);
        }
        catch (RedisServerException ex) when (RedisStreamsErrors.IsGroupExisting(ex))
        {
            // Another instance, an earlier run, or an operator created it first.
        }
    }

    private async Task ConsumeAsync(IDatabase database, RedisStreamsListener listener, CancellationToken stoppingToken)
    {
        var sweepInterval = SweepInterval(Options);
        var lastSweep = Stopwatch.GetTimestamp();
        await SweepAsync(database, listener, stoppingToken).ConfigureAwait(false);
        while (true)
        {
            if (Stopwatch.GetElapsedTime(lastSweep) >= sweepInterval)
            {
                lastSweep = Stopwatch.GetTimestamp();
                await SweepAsync(database, listener, stoppingToken).ConfigureAwait(false);
            }

            var entries = await database.StreamReadGroupAsync(
                    listener.Stream, listener.Group, Options.ConsumerName, StreamPosition.NewMessages, Options.BatchSize)
                .WaitAsync(stoppingToken).ConfigureAwait(false);
            if (entries.Length == 0)
            {
                await Task.Delay(Options.PollInterval, stoppingToken).ConfigureAwait(false);
                continue;
            }

            foreach (var entry in entries)
            {
                await HandleAsync(database, listener, entry, 1, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Takes over entries left unacknowledged too long, whether by a crashed consumer or an earlier failure here.</summary>
    private async Task SweepAsync(IDatabase database, RedisStreamsListener listener, CancellationToken stoppingToken)
    {
        var cursor = StreamStart;
        do
        {
            var result = await database.StreamAutoClaimAsync(
                    listener.Stream,
                    listener.Group,
                    Options.ConsumerName,
                    (long)Options.ClaimIdleAfter.TotalMilliseconds,
                    cursor,
                    Options.BatchSize)
                .WaitAsync(stoppingToken).ConfigureAwait(false);
            if (result.IsNull)
            {
                return;
            }

            var claimed = result.ClaimedEntries.Where(e => !e.IsNull).ToArray();
            var deliveries = await DeliveryCountsAsync(database, listener, claimed, stoppingToken).ConfigureAwait(false);
            foreach (var entry in claimed)
            {
                if (deliveries.TryGetValue(entry.Id, out var attempt))
                {
                    await HandleClaimedAsync(database, listener, entry, attempt, stoppingToken).ConfigureAwait(false);
                }
            }

            cursor = result.NextStartId;
        }
        while (cursor != StreamStart);
    }

    // XPENDING's counter lives on the server, so it survives the consumer crashes reclaiming exists for, unlike a local tally.
    private async Task<Dictionary<RedisValue, int>> DeliveryCountsAsync(
        IDatabase database, RedisStreamsListener listener, StreamEntry[] entries, CancellationToken stoppingToken)
    {
        var lookups = entries.Select(entry => database.StreamPendingMessagesAsync(
            listener.Stream, listener.Group, 1, Options.ConsumerName, entry.Id, entry.Id));
        var pending = await Task.WhenAll(lookups).WaitAsync(stoppingToken).ConfigureAwait(false);
        return pending
            .SelectMany(p => p)
            .ToDictionary(p => p.MessageId, p => p.DeliveryCount);
    }

    private async Task HandleClaimedAsync(IDatabase database, RedisStreamsListener listener, StreamEntry entry, int attempt, CancellationToken stoppingToken)
    {
        LogReclaimed(RedisStreamsMapping.MessageId(listener.Stream, entry), listener.Stream, (string?)entry.Id, attempt);
        if (attempt <= Options.MaxDeliveries)
        {
            await HandleAsync(database, listener, entry, attempt, stoppingToken).ConfigureAwait(false);
            return;
        }

        // Reached only when earlier deliveries never reported back, e.g. the handler kept crashing the process.
        var error = $"Gave up after {attempt - 1} deliveries without an outcome.";
        await DeadLetterAsync(database, listener, entry, error, null, stoppingToken).ConfigureAwait(false);
    }

    private async Task HandleAsync(IDatabase database, RedisStreamsListener listener, StreamEntry entry, int attempt, CancellationToken stoppingToken)
    {
        var messageId = RedisStreamsMapping.MessageId(listener.Stream, entry);
        try
        {
            var message = RedisStreamsMapping.ToIncomingMessage(listener.Stream, entry, attempt);
            await pipeline.ProcessAsync(message, stoppingToken).ConfigureAwait(false);
        }
        catch (PermanentDeliveryException ex)
        {
            await DeadLetterAsync(database, listener, entry, RedisStreamsMapping.Describe(ex), ex, stoppingToken).ConfigureAwait(false);
            return;
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(stoppingToken);
        }
        catch (Exception ex) when (attempt >= Options.MaxDeliveries)
        {
            await DeadLetterAsync(database, listener, entry, RedisStreamsMapping.Describe(ex), ex, stoppingToken).ConfigureAwait(false);
            return;
        }
        catch (Exception ex)
        {
            // Left pending: a sweep reclaims it once it has been idle for ClaimIdleAfter.
            LogProcessingFailed(ex, messageId, listener.Stream, attempt, Options.ClaimIdleAfter);
            return;
        }

        await AcknowledgeAsync(database, listener, entry, messageId).ConfigureAwait(false);
    }

    private async Task DeadLetterAsync(
        IDatabase database, RedisStreamsListener listener, StreamEntry entry, string error, Exception? cause, CancellationToken stoppingToken)
    {
        var messageId = RedisStreamsMapping.MessageId(listener.Stream, entry);
        try
        {
            await database.StreamAddAsync(listener.DeadStream, RedisStreamsMapping.ToDeadLetter(listener.Stream, listener.Group, entry, error))
                .WaitAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(stoppingToken);
        }
        catch (Exception ex)
        {
            // Acknowledging without the copy would lose the entry, so it stays pending and is reclaimed later.
            LogDeadLetterFailed(ex, messageId, listener.Stream, listener.DeadStream);
            return;
        }

        LogDeadLettered(cause, messageId, listener.Stream, listener.DeadStream, error);
        await AcknowledgeAsync(database, listener, entry, messageId).ConfigureAwait(false);
    }

    private async Task AcknowledgeAsync(IDatabase database, RedisStreamsListener listener, StreamEntry entry, string messageId)
    {
        try
        {
            await database.StreamAcknowledgeAsync(listener.Stream, listener.Group, entry.Id).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The entry stays pending and is reclaimed; the inbox absorbs the repeat.
            LogAcknowledgeFailed(ex, messageId, listener.Stream);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming Redis stream {Stream} as {Consumer} in group {Group}.")]
    private partial void LogListening(string stream, string group, string consumer);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created consumer group {Group} on Redis stream {Stream}.")]
    private partial void LogGroupCreated(string stream, string group);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Consumer for Redis stream {Stream} in group {Group} failed; restarting it in {Delay}.")]
    private partial void LogListenerFailed(Exception error, string stream, string group, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Reclaimed message {MessageId} ({Stream} {EntryId}) for delivery {Attempt}.")]
    private partial void LogReclaimed(string messageId, string stream, string? entryId, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageId} from {Stream} failed on delivery {Attempt}; it is retried once idle for {ClaimIdleAfter}.")]
    private partial void LogProcessingFailed(Exception error, string messageId, string stream, int attempt, TimeSpan claimIdleAfter);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Stream} moved to {DeadStream}: {Reason}")]
    private partial void LogDeadLettered(Exception? error, string messageId, string stream, string deadStream, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not move message {MessageId} from {Stream} to {DeadStream}; retrying it later.")]
    private partial void LogDeadLetterFailed(Exception error, string messageId, string stream, string deadStream);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not acknowledge message {MessageId} on {Stream}; it may be delivered again.")]
    private partial void LogAcknowledgeFailed(Exception error, string messageId, string stream);
}
