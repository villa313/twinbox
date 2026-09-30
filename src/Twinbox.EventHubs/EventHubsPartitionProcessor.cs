using Azure.Core;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Messaging.EventHubs.Processor;
using Microsoft.Extensions.Logging;

namespace Twinbox.EventHubs;

// Built on the batch-capable primitive because the stock processor client hands over one event at a time. Each owned
// partition is read in its own loop that awaits a batch before reading more, so a retry stalls only that partition.
internal sealed partial class EventHubsPartitionProcessor : PluggableCheckpointStoreEventProcessor<EventProcessorPartition>
{
    private readonly EventHubsEventHandler _handler;
    private readonly ILogger _logger;

    public EventHubsPartitionProcessor(
        CheckpointStore checkpointStore,
        int maxBatchSize,
        EventHubsListener listener,
        string connectionString,
        EventProcessorOptions options,
        EventHubsEventHandler handler,
        ILogger logger)
        : base(checkpointStore, maxBatchSize, listener.ConsumerGroup, connectionString, listener.EventHub, options)
    {
        _handler = handler;
        _logger = logger;
    }

    public EventHubsPartitionProcessor(
        CheckpointStore checkpointStore,
        int maxBatchSize,
        EventHubsListener listener,
        string fullyQualifiedNamespace,
        TokenCredential credential,
        EventProcessorOptions options,
        EventHubsEventHandler handler,
        ILogger logger)
        : base(checkpointStore, maxBatchSize, listener.ConsumerGroup, fullyQualifiedNamespace, listener.EventHub, credential, options)
    {
        _handler = handler;
        _logger = logger;
    }

    protected override async Task OnProcessingEventBatchAsync(
        IEnumerable<EventData> events,
        EventProcessorPartition partition,
        CancellationToken cancellationToken)
    {
        var context = new PartitionContext(
            EventHubName,
            partition.PartitionId,
            (data, token) => UpdateCheckpointAsync(partition.PartitionId, CheckpointPosition.FromEvent(data), token));
        try
        {
            await _handler.HandleAsync(context, events as IReadOnlyList<EventData> ?? [.. events], cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The partition was released or the processor is stopping; its next owner resumes from the last checkpoint.
        }
    }

    protected override Task OnProcessingErrorAsync(
        Exception exception,
        EventProcessorPartition partition,
        string operationDescription,
        CancellationToken cancellationToken)
    {
        LogProcessorError(exception, EventHubName, ConsumerGroup, partition?.PartitionId ?? "-", operationDescription);
        return Task.CompletedTask;
    }

    protected override Task OnInitializingPartitionAsync(EventProcessorPartition partition, CancellationToken cancellationToken)
    {
        LogPartitionClaimed(EventHubName, ConsumerGroup, partition.PartitionId);
        return Task.CompletedTask;
    }

    protected override Task OnPartitionProcessingStoppedAsync(
        EventProcessorPartition partition,
        ProcessingStoppedReason reason,
        CancellationToken cancellationToken)
    {
        LogPartitionReleased(EventHubName, ConsumerGroup, partition.PartitionId, reason);
        return Task.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Event Hubs processor for {EventHub} ({ConsumerGroup}) partition {PartitionId} reported an error during {Operation}.")]
    private partial void LogProcessorError(Exception error, string eventHub, string consumerGroup, string partitionId, string operation);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Event Hubs {EventHub} ({ConsumerGroup}) partition {PartitionId} claimed.")]
    private partial void LogPartitionClaimed(string eventHub, string consumerGroup, string partitionId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Event Hubs {EventHub} ({ConsumerGroup}) partition {PartitionId} released: {Reason}.")]
    private partial void LogPartitionReleased(string eventHub, string consumerGroup, string partitionId, ProcessingStoppedReason reason);
}
