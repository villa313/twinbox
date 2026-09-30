using Azure.Messaging.EventHubs.Primitives;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Twinbox.EventHubs;

/// <summary>Runs one partition processor per listener; each processor balances partitions with other instances of the app.</summary>
internal sealed partial class EventHubsProcessorService(
    EventHubsClients clients,
    EventHubsEventHandler handler,
    ILoggerFactory loggerFactory,
    ILogger<EventHubsProcessorService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRestartDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger = logger;
    private readonly List<EventHubsPartitionProcessor> _processors = [];
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes once every listener's processor has started.</summary>
    public Task Ready => _ready.Task;

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Waits for ExecuteAsync first, so the processor list is no longer being filled.
        try
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown timed out; still stop the processors below rather than failing the host's shutdown.
        }

        EventHubsPartitionProcessor[] processors;
        lock (_processors)
        {
            processors = [.. _processors];
            _processors.Clear();
        }

        foreach (var processor in processors)
        {
            try
            {
                await processor.StopProcessingAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogStopFailed(ex, processor.EventHubName);
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        stoppingToken.Register(() => _ready.TrySetCanceled(stoppingToken));
        await Task.WhenAll(clients.Options.Listeners.Select(listener => StartAsync(listener, stoppingToken))).ConfigureAwait(false);
        _ready.TrySetResult();
    }

    internal static EventHubsPartitionProcessor CreateProcessor(
        EventHubsOptions options,
        EventHubsListener listener,
        CheckpointStore checkpointStore,
        EventHubsEventHandler handler,
        ILogger logger)
    {
        var processorOptions = EventHubsClients.CreateProcessorOptions(options);
        return options.ConnectionString is { Length: > 0 } connectionString
            ? new EventHubsPartitionProcessor(checkpointStore, options.MaxBatchSize, listener, connectionString, processorOptions, handler, logger)
            : new EventHubsPartitionProcessor(
                checkpointStore, options.MaxBatchSize, listener, options.FullyQualifiedNamespace!, options.Credential!, processorOptions, handler, logger);
    }

    /// <summary>Starting checks the event hub and checkpoint container are reachable, so it is retried until it succeeds or the host stops.</summary>
    private async Task StartAsync(EventHubsListener listener, CancellationToken stoppingToken)
    {
        var restartDelay = InitialRestartDelay;
        while (true)
        {
            try
            {
                var container = listener.CheckpointContainer(clients.Options);
                if (clients.Options.CreateCheckpointContainers)
                {
                    await container.CreateIfNotExistsAsync(cancellationToken: stoppingToken).ConfigureAwait(false);
                }

                var processor = CreateProcessor(
                    clients.Options, listener, new BlobCheckpointStore(container), handler, loggerFactory.CreateLogger<EventHubsPartitionProcessor>());
                await processor.StartProcessingAsync(stoppingToken).ConfigureAwait(false);
                lock (_processors)
                {
                    _processors.Add(processor);
                }

                LogListening(listener.EventHub, listener.ConsumerGroup);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                LogStartFailed(ex, listener.EventHub, listener.ConsumerGroup, restartDelay);
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

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming Event Hubs {EventHub} as consumer group {ConsumerGroup}.")]
    private partial void LogListening(string eventHub, string consumerGroup);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not start consuming Event Hubs {EventHub} as {ConsumerGroup}; retrying in {Delay}.")]
    private partial void LogStartFailed(Exception error, string eventHub, string consumerGroup, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Stopping the Event Hubs processor for {EventHub} failed.")]
    private partial void LogStopFailed(Exception error, string eventHub);
}
