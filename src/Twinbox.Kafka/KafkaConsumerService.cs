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

    private readonly ILogger _logger = logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _unsubscribed;

    /// <summary>Completes once every listener has subscribed.</summary>
    public Task Ready => _ready.Task;

    /// <summary>Doubles <see cref="KafkaOptions.RetryDelay"/> per failed attempt, capped at <see cref="KafkaOptions.MaxRetryDelay"/>.</summary>
    internal static TimeSpan RetryDelay(KafkaOptions options, int attempt)
    {
        var factor = Math.Pow(2, Math.Clamp(attempt - 1, 0, 30));
        var ticks = Math.Min(options.RetryDelay.Ticks * factor, options.MaxRetryDelay.Ticks);
        return TimeSpan.FromTicks((long)ticks);
    }

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
                var attempts = new Dictionary<TopicPartition, RecordAttempt>();
                using var consumer = clients.CreateConsumer(
                    listener,
                    (_, partitions) => LogAssigned(listener.Topic, listener.GroupId, string.Join(", ", partitions.Select(p => p.Partition.Value))),
                    (_, partitions) => Forget(attempts, partitions));
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
                    Consume(consumer, attempts, stoppingToken);
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

    private void Consume(IConsumer<string?, byte[]> consumer, Dictionary<TopicPartition, RecordAttempt> attempts, CancellationToken stoppingToken)
    {
        while (true)
        {
            ConsumeResult<string?, byte[]> record;
            try
            {
                record = consumer.Consume(stoppingToken);
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                LogConsumeFailed(ex, ex.Error.Code);
                continue;
            }

            if (record is null || record.IsPartitionEOF)
            {
                continue;
            }

            var attempt = attempts.TryGetValue(record.TopicPartition, out var previous) && previous.Offset == record.Offset.Value
                ? previous.Attempt + 1
                : 1;
            attempts[record.TopicPartition] = new RecordAttempt(record.Offset.Value, attempt);

            if (Process(record, attempt, stoppingToken))
            {
                attempts.Remove(record.TopicPartition);
                Commit(consumer, record);
            }
            else
            {
                Rewind(consumer, record, attempts);
                if (stoppingToken.WaitHandle.WaitOne(RetryDelay(clients.Options, attempt)))
                {
                    throw new OperationCanceledException(stoppingToken);
                }
            }
        }
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
            return DeadLetter(record, message.MessageId, ex, stoppingToken);
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

    private bool DeadLetter(ConsumeResult<string?, byte[]> record, string messageId, PermanentDeliveryException error, CancellationToken stoppingToken)
    {
        if (clients.Options.DeadLetterTopic is not { } deadLetterTopic)
        {
            LogSkipped(error, messageId, record.Topic, record.Partition.Value, record.Offset.Value);
            return true;
        }

        try
        {
            clients.EnsureTopicAsync(deadLetterTopic, stoppingToken).GetAwaiter().GetResult();
            clients.ProduceAsync(deadLetterTopic, KafkaMapping.ToDeadLetter(record, error), stoppingToken).GetAwaiter().GetResult();
            LogDeadLettered(error, messageId, record.Topic, deadLetterTopic);
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

    // Committing each record synchronously bounds redelivery after a crash or rebalance to the record in flight,
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

    /// <summary>Seeks back so the next Consume returns the same record, keeping later records of the partition behind it.</summary>
    private void Rewind(IConsumer<string?, byte[]> consumer, ConsumeResult<string?, byte[]> record, Dictionary<TopicPartition, RecordAttempt> attempts)
    {
        try
        {
            consumer.Seek(record.TopicPartitionOffset);
        }
        catch (KafkaException ex)
        {
            // Usually the partition was revoked meanwhile; its new owner resumes from the last committed offset.
            attempts.Remove(record.TopicPartition);
            LogRewindFailed(ex, record.Topic, record.Partition.Value, record.Offset.Value);
        }
    }

    private void Forget(Dictionary<TopicPartition, RecordAttempt> attempts, List<TopicPartitionOffset> partitions)
    {
        foreach (var partition in partitions)
        {
            attempts.Remove(partition.TopicPartition);
        }

        LogRevoked(string.Join(", ", partitions.Select(p => p.TopicPartition.ToString())));
    }

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

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Topic} failed permanently; copied it to {DeadLetterTopic}.")]
    private partial void LogDeadLettered(Exception error, string messageId, string topic, string deadLetterTopic);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Topic} [{Partition}] @{Offset} failed permanently and no dead-letter topic is set; skipping it.")]
    private partial void LogSkipped(Exception error, string messageId, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not copy message {MessageId} from {Topic} to {DeadLetterTopic}; retrying it.")]
    private partial void LogDeadLetterFailed(Exception error, string messageId, string topic, string deadLetterTopic);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not commit {Topic} [{Partition}] @{Offset}; the record may be redelivered.")]
    private partial void LogCommitFailed(Exception error, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Could not seek back to {Topic} [{Partition}] @{Offset}.")]
    private partial void LogRewindFailed(Exception error, string topic, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Closing the Kafka consumer for {Topic} failed.")]
    private partial void LogCloseFailed(Exception error, string topic);

    private readonly record struct RecordAttempt(long Offset, int Attempt);
}
