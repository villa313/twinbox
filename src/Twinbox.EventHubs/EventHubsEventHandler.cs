using Azure.Messaging.EventHubs;
using Microsoft.Extensions.Logging;
using Twinbox.Transport;

namespace Twinbox.EventHubs;

/// <summary>Where a batch of events came from, and how to record that the partition has been handled up to an event.</summary>
internal sealed record PartitionContext(string EventHub, string PartitionId, Func<EventData, CancellationToken, Task> Checkpoint);

/// <summary>Feeds one partition's events through the inbox, retrying a failed event in place so only its own partition waits.</summary>
internal sealed partial class EventHubsEventHandler(EventHubsClients clients, IInboundPipeline pipeline, ILogger<EventHubsEventHandler> logger)
{
    private readonly ILogger _logger = logger;

    /// <summary>Doubles <see cref="EventHubsOptions.RetryDelay"/> per failed attempt, capped at <see cref="EventHubsOptions.MaxRetryDelay"/>.</summary>
    internal static TimeSpan RetryDelay(EventHubsOptions options, int attempt)
    {
        var factor = Math.Pow(2, Math.Clamp(attempt - 1, 0, 30));
        var ticks = Math.Min(options.RetryDelay.Ticks * factor, options.MaxRetryDelay.Ticks);
        return TimeSpan.FromTicks((long)ticks);
    }

    /// <summary>Returns once every event is handled and checkpointed; throws <see cref="OperationCanceledException"/> when the partition stops first.</summary>
    public async Task HandleAsync(PartitionContext partition, IReadOnlyList<EventData> events, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(partition);
        ArgumentNullException.ThrowIfNull(events);
        if (events.Count == 0)
        {
            return;
        }

        if (events.Count > 1 && await ProcessBatchAsync(partition, events, cancellationToken).ConfigureAwait(false))
        {
            await CheckpointAsync(partition, events[^1]).ConfigureAwait(false);
            return;
        }

        foreach (var data in events)
        {
            await ProcessUntilDoneAsync(partition, data, cancellationToken).ConfigureAwait(false);
            await CheckpointAsync(partition, data).ConfigureAwait(false);
        }
    }

    /// <summary>True when the whole batch was handled; false means isolate the failure event by event.</summary>
    private async Task<bool> ProcessBatchAsync(PartitionContext partition, IReadOnlyList<EventData> events, CancellationToken cancellationToken)
    {
        var messages = events.Select(e => EventHubsMapping.ToIncomingMessage(e, partition.EventHub, partition.PartitionId)).ToList();
        try
        {
            await pipeline.ProcessBatchAsync(messages, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception ex)
        {
            LogBatchFailed(ex, events.Count, partition.EventHub, partition.PartitionId, events[0].SequenceNumber, events[^1].SequenceNumber);
            return false;
        }
    }

    // Event Hubs keeps no delivery count, so attempts are counted here; a restart begins again at 1.
    private async Task ProcessUntilDoneAsync(PartitionContext partition, EventData data, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            if (await ProcessAsync(partition, data, attempt, cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            await Task.Delay(RetryDelay(clients.Options, attempt), cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>True when the event is done with (handled, dead-lettered or skipped) and can be checkpointed past.</summary>
    private async Task<bool> ProcessAsync(PartitionContext partition, EventData data, int attempt, CancellationToken cancellationToken)
    {
        var message = EventHubsMapping.ToIncomingMessage(data, partition.EventHub, partition.PartitionId, attempt);
        try
        {
            await pipeline.ProcessAsync(message, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (PermanentDeliveryException ex)
        {
            return await DeadLetterAsync(partition, data, message.MessageId, ex, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception ex)
        {
            LogProcessingFailed(ex, message.MessageId, partition.EventHub, partition.PartitionId, data.SequenceNumber, attempt);
            return false;
        }
    }

    private async Task<bool> DeadLetterAsync(
        PartitionContext partition,
        EventData data,
        string messageId,
        PermanentDeliveryException error,
        CancellationToken cancellationToken)
    {
        if (clients.Options.DeadLetterEventHub is not { } deadLetterEventHub)
        {
            LogSkipped(error, messageId, partition.EventHub, partition.PartitionId, data.SequenceNumber);
            return true;
        }

        try
        {
            var copy = EventHubsMapping.ToDeadLetter(data, partition.EventHub, partition.PartitionId, error);
            await clients.SendAsync(deadLetterEventHub, copy, data.PartitionKey, cancellationToken).ConfigureAwait(false);
            LogDeadLettered(error, messageId, partition.EventHub, deadLetterEventHub);
            return true;
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (Exception ex)
        {
            // Checkpointing past the event without the copy would lose it, so it is retried like a handler failure.
            LogDeadLetterFailed(ex, messageId, partition.EventHub, deadLetterEventHub);
            return false;
        }
    }

    // Not cancellable, so a handled event is recorded even while its partition stops. A failed write only means
    // redelivery after a restart or ownership change, which the inbox deduplicates.
    private async Task CheckpointAsync(PartitionContext partition, EventData data)
    {
        try
        {
            await partition.Checkpoint(data, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogCheckpointFailed(ex, partition.EventHub, partition.PartitionId, data.SequenceNumber);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Event {MessageId} from {EventHub} [{PartitionId}] #{SequenceNumber} failed on attempt {Attempt}; retrying it.")]
    private partial void LogProcessingFailed(Exception error, string messageId, string eventHub, string partitionId, long sequenceNumber, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "A batch of {Count} events from {EventHub} [{PartitionId}] #{FirstSequenceNumber}-{LastSequenceNumber} failed; processing them one by one.")]
    private partial void LogBatchFailed(Exception error, int count, string eventHub, string partitionId, long firstSequenceNumber, long lastSequenceNumber);

    [LoggerMessage(Level = LogLevel.Error, Message = "Event {MessageId} from {EventHub} failed permanently; copied it to {DeadLetterEventHub}.")]
    private partial void LogDeadLettered(Exception error, string messageId, string eventHub, string deadLetterEventHub);

    [LoggerMessage(Level = LogLevel.Error, Message = "Event {MessageId} from {EventHub} [{PartitionId}] #{SequenceNumber} failed permanently and no dead-letter event hub is set; skipping it.")]
    private partial void LogSkipped(Exception error, string messageId, string eventHub, string partitionId, long sequenceNumber);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not copy event {MessageId} from {EventHub} to {DeadLetterEventHub}; retrying it.")]
    private partial void LogDeadLetterFailed(Exception error, string messageId, string eventHub, string deadLetterEventHub);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not checkpoint {EventHub} [{PartitionId}] #{SequenceNumber}; the event may be redelivered.")]
    private partial void LogCheckpointFailed(Exception error, string eventHub, string partitionId, long sequenceNumber);
}
