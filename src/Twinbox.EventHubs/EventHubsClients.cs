using System.Collections.Concurrent;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Options;

namespace Twinbox.EventHubs;

/// <summary>Owns one producer per event hub, shared by sending and dead-lettering, and builds listener processor options.</summary>
internal sealed class EventHubsClients(IOptions<EventHubsOptions> options) : IAsyncDisposable, IDisposable
{
    private readonly ConcurrentDictionary<string, Lazy<EventHubProducerClient>> _producers = new(StringComparer.Ordinal);
    private bool _disposed;

    public EventHubsOptions Options { get; } = options.Value;

    /// <summary>Sends one event as a batch of one, so an oversized event is caught before it reaches the service.</summary>
    public async Task SendAsync(string eventHub, EventData data, string? partitionKey, CancellationToken cancellationToken)
    {
        var producer = GetProducer(eventHub);
        using var batch = await producer.CreateBatchAsync(new CreateBatchOptions { PartitionKey = partitionKey }, cancellationToken)
            .ConfigureAwait(false);
        if (!batch.TryAdd(data))
        {
            throw new EventHubsException(
                isTransient: false,
                eventHub,
                $"The event is larger than the {batch.MaximumSizeInBytes} bytes '{eventHub}' accepts.",
                EventHubsException.FailureReason.MessageSizeExceeded);
        }

        await producer.SendAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        foreach (var producer in _producers.Values.Where(p => p.IsValueCreated))
        {
            await producer.Value.DisposeAsync().ConfigureAwait(false);
        }

        _producers.Clear();
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    internal static EventHubProducerClientOptions CreateProducerOptions(EventHubsOptions options)
    {
        var producerOptions = new EventHubProducerClientOptions();
        options.ConfigureProducer?.Invoke(producerOptions);
        return producerOptions;
    }

    internal static EventProcessorOptions CreateProcessorOptions(EventHubsOptions options)
    {
        var processorOptions = new EventProcessorOptions();

        // A read never returns more than is prefetched, so a smaller prefetch would silently cap batches.
        processorOptions.PrefetchCount = Math.Max(processorOptions.PrefetchCount, options.MaxBatchSize);
        options.ConfigureProcessor?.Invoke(processorOptions);
        return processorOptions;
    }

    private EventHubProducerClient GetProducer(string eventHub)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _producers.GetOrAdd(eventHub, static (name, self) => new Lazy<EventHubProducerClient>(() => self.CreateProducer(name)), this).Value;
    }

    private EventHubProducerClient CreateProducer(string eventHub)
    {
        var producerOptions = CreateProducerOptions(Options);
        return Options.ConnectionString is { Length: > 0 } connectionString
            ? new EventHubProducerClient(connectionString, eventHub, producerOptions)
            : new EventHubProducerClient(Options.FullyQualifiedNamespace, eventHub, Options.Credential, producerOptions);
    }
}
