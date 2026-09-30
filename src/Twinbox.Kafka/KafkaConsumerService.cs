using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.Transport;

namespace Twinbox.Kafka;

/// <summary>Runs one consumer per listener, each on a dedicated thread because Consume blocks.</summary>
internal sealed partial class KafkaConsumerService(
    KafkaClients clients,
    IInboundPipeline pipeline,
    ILogger<KafkaConsumerService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRestartDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);

    // Bounds how long a poll blocks, so stopping and due resumes are noticed promptly.
    private static readonly TimeSpan PollTimeout = TimeSpan.FromMilliseconds(100);

    private readonly ILogger _logger = logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _unsubscribed;

    /// <summary>Completes once every listener has subscribed.</summary>
    public Task Ready => _ready.Task;

    internal static TimeSpan RetryDelay(KafkaOptions options, int attempt) =>
        RetryBackoff.Delay(options.RetryDelay, options.MaxRetryDelay, attempt);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listeners = clients.Options.Listeners;
        _unsubscribed = listeners.Count;
        if (listeners.Count == 0)
        {
            _ready.TrySetResult();
            return Task.CompletedTask;
        }

        stoppingToken.Register(() => _ready.TrySetCanceled(stoppingToken));
        var workers = listeners.Select(listener => Task.Factory.StartNew(
            () => Run(listener, stoppingToken),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default));
        return Task.WhenAll(workers);
    }

    /// <summary>Keeps a consumer alive for the listener, rebuilding it after failures until the host stops.</summary>
    private void Run(KafkaListener listener, CancellationToken stoppingToken)
    {
        var restartDelay = InitialRestartDelay;
        var subscribed = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                EnsureTopics(listener, stoppingToken);
                var retries = new PartitionRetries();
                var batches = new PartitionBatches(clients.Options.MaxBatchSize);
                using var consumer = clients.CreateConsumer(
                    listener,
                    (_, partitions) => LogAssigned(listener.Topic, listener.GroupId, string.Join(", ", partitions.Select(p => p.Partition.Value))),
                    (c, partitions) => Forget(c, retries, batches, partitions));
                consumer.Subscribe(listener.Topic);
                LogListening(listener.Topic, listener.GroupId);
                if (!subscribed)
                {
                    subscribed = true;
                    if (Interlocked.Decrement(ref _unsubscribed) == 0)
                    {
                        _ready.TrySetResult();
                    }
                }

                restartDelay = InitialRestartDelay;
                try
                {
                    Consume(consumer, retries, batches, stoppingToken);
                }
                finally
                {
                    Close(consumer, listener);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                LogListenerFailed(ex, listener.Topic, restartDelay);
            }

            if (stoppingToken.WaitHandle.WaitOne(restartDelay))
            {
                return;
            }

            restartDelay = TimeSpan.FromTicks(Math.Min(restartDelay.Ticks * 2, MaxRestartDelay.Ticks));
        }
    }

    private void Consume(IConsumer<string?, byte[]> consumer, PartitionRetries retries, PartitionBatches batches, CancellationToken stoppingToken)
    {
        while (true)
        {
            // Resuming throws only on a broken consumer, which the restart in Run replaces.
            if (retries.TakeDue(Now) is { Count: > 0 } due)
            {
                consumer.Resume(due);
            }

            Collect(consumer, retries, batches, stoppingToken);
            foreach (var batch in batches.TakeAll())
            {
                ProcessPartition(consumer, batch, retries, stoppingToken);
            }
        }
    }

    /// <summary>Takes the next record, plus up to a batch of whatever else is already available within MaxBatchWait.</summary>
    private void Collect(IConsumer<string?, byte[]> consumer, PartitionRetries retries, PartitionBatches batches, CancellationToken stoppingToken)
    {
        var first = Poll(consumer, retries, retries.UntilNextResume(Now, PollTimeout), stoppingToken);
        if (first is null || batches.Add(first))
        {
            return;
        }

        var deadline = Now + (long)clients.Options.MaxBatchWait.TotalMilliseconds;
        while (Poll(consumer, retries, TimeSpan.FromMilliseconds(Math.Max(0, deadline - Now)), stoppingToken) is { } record)
        {
            if (batches.Add(record))
            {
                return;
            }
        }
    }

    private ConsumeResult<string?, byte[]>? Poll(
        IConsumer<string?, byte[]> consumer,
        PartitionRetries retries,
        TimeSpan timeout,
        CancellationToken stoppingToken)
    {
        stoppingToken.ThrowIfCancellationRequested();
        ConsumeResult<string?, byte[]>? record;
        try
        {
            record = consumer.Consume(timeout);
        }
        catch (ConsumeException ex) when (!ex.Error.IsFatal)
        {
            LogConsumeFailed(ex, ex.Error.Code);
            return null;
        }

        // A paused partition was sought back, so a record fetched before the pause is read again after it resumes.
        return record is null || record.IsPartitionEOF || retries.IsPaused(record.TopicPartition) ? null : record;
    }

    private void ProcessPartition(
        IConsumer<string?, byte[]> consumer,
        List<ConsumeResult<string?, byte[]>> records,
        PartitionRetries retries,
        CancellationToken stoppingToken)
    {
        var start = 0;

        // A record that already failed goes alone, so it can't sink the batch around it again.
        if (records.Count > 1 && retries.AttemptOf(records[0].TopicPartitionOffset) > 1)
        {
            if (!ProcessOne(consumer, records[0], retries, stoppingToken))
            {
                return;
            }

            start = 1;
        }

        var rest = records.GetRange(start, records.Count - start);
        if (rest.Count > 1 && ProcessBatch(consumer, rest, retries, stoppingToken))
        {
            return;
        }

        foreach (var record in rest)
        {
            if (!ProcessOne(consumer, record, retries, stoppingToken))
            {
                return;
            }
        }
    }

    /// <summary>True when the whole batch was handled and committed; false means isolate the failure record by record.</summary>
    private bool ProcessBatch(
        IConsumer<string?, byte[]> consumer,
        List<ConsumeResult<string?, byte[]>> records,
        PartitionRetries retries,
        CancellationToken stoppingToken)
    {
        var messages = records.ConvertAll(r => KafkaMapping.ToIncomingMessage(r, retries.AttemptOf(r.TopicPartitionOffset)));
        var last = records[^1];
        try
        {
            pipeline.ProcessBatchAsync(messages, stoppingToken).GetAwaiter().GetResult();
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(stoppingToken);
        }
        catch (Exception ex)
        {
            LogBatchFailed(ex, records.Count, last.Topic, last.Partition.Value, records[0].Offset.Value, last.Offset.Value);
            return false;
        }

        retries.Succeeded(last.TopicPartition);
        Commit(consumer, last);
        return true;
    }

    /// <summary>True when the record is done with and committed; false when its partition now waits to retry it.</summary>
    private bool ProcessOne(
        IConsumer<string?, byte[]> consumer,
        ConsumeResult<string?, byte[]> record,
        PartitionRetries retries,
        CancellationToken stoppingToken)
    {
        var attempt = retries.AttemptOf(record.TopicPartitionOffset);
        if (Process(record, attempt, stoppingToken))
        {
            retries.Succeeded(record.TopicPartition);
            Commit(consumer, record);
            return true;
        }

        Pause(consumer, record, attempt, retries);
        return false;
    }

    /// <summary>True when the record is done with (handled, dead-lettered or skipped) and its offset can be committed.</summary>
    private bool Process(ConsumeResult<string?, byte[]> record, int attempt, CancellationToken stoppingToken)
    {
        var message = KafkaMapping.ToIncomingMessage(record, attempt);
        try
        {
            // Blocking is safe here: this is the listener's own thread, with no synchronization context.
            pipeline.ProcessAsync(message, stoppingToken).GetAwaiter().GetResult();
            return true;
        }
        catch (PermanentDeliveryException ex)
        {
            return DeadLetter(record, message.MessageId, message.MessageName, ex, stoppingToken);
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(stoppingToken);
        }
        catch (Exception ex)
        {
            LogProcessingFailed(ex, message.MessageId, record.Topic, record.Partition.Value, record.Offset.Value, attempt);
            return false;
        }
    }

    private bool DeadLetter(
        ConsumeResult<string?, byte[]> record,
        string messageId,
        string messageName,
        PermanentDeliveryException error,
        CancellationToken stoppingToken)
    {
        if (clients.Options.DeadLetterTopic is not { } deadLetterTopic)
        {
            LogSkipped(error, messageId, messageName, record.Topic, record.Partition.Value, record.Offset.Value);
            InboundDiagnostics.RecordDiscarded(KafkaTransport.TransportName, record.Topic);
            return true;
        }

        try
        {
            clients.EnsureTopicAsync(deadLetterTopic, stoppingToken).GetAwaiter().GetResult();
            clients.ProduceAsync(deadLetterTopic, KafkaMapping.ToDeadLetter(record, error), stoppingToken).GetAwaiter().GetResult();
            LogDeadLettered(error, messageId, record.Topic, deadLetterTopic);
            InboundDiagnostics.RecordDeadLettered(KafkaTransport.TransportName, record.Topic);
            return true;
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(stoppingToken);
        }
        catch (Exception ex)
        {
            // Committing without the copy would lose the record, so it is retried like a handler failure.
            LogDeadLetterFailed(ex, messageId, record.Topic, deadLetterTopic);
            return false;
        }
    }

    // Committing each record or batch synchronously bounds redelivery after a crash or rebalance to what was in flight,
    // which the inbox deduplicates; it trades throughput for not having to track stored offsets across rebalances.
    private void Commit(IConsumer<string?, byte[]> consumer, ConsumeResult<string?, byte[]> record)
    {
        try
        {
            consumer.Commit(record);
        }
        catch (KafkaException ex)
        {
            LogCommitFailed(ex, record.Topic, record.Partition.Value, record.Offset.Value);
        }
    }

    /// <summary>Pauses only the record's partition and seeks it back, so its later records wait while other partitions flow.</summary>
    private void Pause(IConsumer<string?, byte[]> consumer, ConsumeResult<string?, byte[]> record, int attempt, PartitionRetries retries)
    {
        try
        {
            consumer.Pause([record.TopicPartition]);
            consumer.Seek(record.TopicPartitionOffset);
        }
        catch (KafkaException ex) when (!consumer.Assignment.Contains(record.TopicPartition))
        {
            // Revoked meanwhile; its new owner resumes from the last committed offset. Still assigned, the error
            // propagates instead and the rebuilt consumer resumes from that offset, so the record is never skipped.
            retries.Forget(record.TopicPartition);
            LogRewindFailed(ex, record.Topic, record.Partition.Value, record.Offset.Value);
            return;
        }

        retries.Failed(record.TopicPartitionOffset, attempt, Now + (long)RetryDelay(clients.Options, attempt).TotalMilliseconds);
    }

    private void Forget(IConsumer<string?, byte[]> consumer, PartitionRetries retries, PartitionBatches batches, List<TopicPartitionOffset> partitions)
    {
        var paused = new List<TopicPartition>();
        foreach (var partition in partitions)
        {
            batches.Drop(partition.TopicPartition);
            if (retries.Forget(partition.TopicPartition))
            {
                paused.Add(partition.TopicPartition);
            }
        }

        if (paused.Count > 0)
        {
            try
            {
                // So the partition starts unpaused if it is assigned back later.
                consumer.Resume(paused);
            }
            catch (KafkaException ex)
            {
                LogResumeFailed(ex, string.Join(", ", paused));
            }
        }

        LogRevoked(string.Join(", ", partitions.Select(p => p.TopicPartition.ToString())));
    }

    private static long Now => Environment.TickCount64;

    private void EnsureTopics(KafkaListener listener, CancellationToken stoppingToken)
    {
        clients.EnsureTopicAsync(listener.Topic, stoppingToken).GetAwaiter().GetResult();
        if (clients.Options.DeadLetterTopic is { } deadLetterTopic)
        {
            clients.EnsureTopicAsync(deadLetterTopic, stoppingToken).GetAwaiter().GetResult();
        }
    }

    private void Close(IConsumer<string?, byte[]> consumer, KafkaListener listener)
    {
        try
        {
            consumer.Close();
        }
        catch (KafkaException ex)
        {
            LogCloseFailed(ex, listener.Topic);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming Kafka topic {Topic} as group {GroupId}.")]
    private partial void LogListening(string topic, string groupId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Kafka topic {Topic} partitions [{Partitions}] assigned to group {GroupId}.")]
    private partial void LogAssigned(string topic, string groupId, string partitions);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Kafka partitions [{Partitions}] revoked.")]
    private partial void LogRevoked(string partitions);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka consumer for {Topic} failed; restarting it in {Delay}.")]
    private partial void LogListenerFailed(Exception error, string topic, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka consume reported {Code}.")]
    private partial void LogConsumeFailed(Exception error, ErrorCode code);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageId} from {Topic} [{Partition}] @{Offset} failed on attempt {Attempt}; retrying it.")]
    private partial void LogProcessingFailed(Exception error, string messageId, string topic, int partition, long offset, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "A batch of {Count} messages from {Topic} [{Partition}] @{FirstOffset}-{LastOffset} failed; processing them one by one.")]
    private partial void LogBatchFailed(Exception error, int count, string topic, int partition, long firstOffset, long lastOffset);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Topic} failed permanently; copied it to {DeadLetterTopic}.")]
    private partial void LogDeadLettered(Exception error, string messageId, string topic, string deadLetterTopic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} ('{MessageName}') from {Topic} [{Partition}] @{Offset} failed permanently and no dead-letter topic is set; skipping it.")]
    private partial void LogSkipped(Exception error, string messageId, string messageName, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not copy message {MessageId} from {Topic} to {DeadLetterTopic}; retrying it.")]
    private partial void LogDeadLetterFailed(Exception error, string messageId, string topic, string deadLetterTopic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not commit {Topic} [{Partition}] @{Offset}; the record may be redelivered.")]
    private partial void LogCommitFailed(Exception error, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Could not seek back to {Topic} [{Partition}] @{Offset}.")]
    private partial void LogRewindFailed(Exception error, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Could not resume revoked Kafka partitions [{Partitions}].")]
    private partial void LogResumeFailed(Exception error, string partitions);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Closing the Kafka consumer for {Topic} failed.")]
    private partial void LogCloseFailed(Exception error, string topic);
}
