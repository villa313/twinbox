using Azure.Core;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;

namespace Twinbox.EventHubs;

public sealed class EventHubsOptions
{
    private readonly List<EventHubsListener> _listeners = [];

    /// <summary>Namespace connection string; leave unset to authenticate with <see cref="Credential"/> instead.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>E.g. "my-namespace.servicebus.windows.net"; used with <see cref="Credential"/>.</summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>Token credential such as DefaultAzureCredential, also used for checkpoint containers given by URI.</summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>Last say over each producer's options, e.g. retry or transport settings.</summary>
    public Action<EventHubProducerClientOptions>? ConfigureProducer { get; set; }

    /// <summary>Last say over each listener's processor options, e.g. load balancing or the default starting position.</summary>
    public Action<EventProcessorOptions>? ConfigureProcessor { get; set; }

    /// <summary>Create missing checkpoint containers when a listener starts; needs permission to create containers.</summary>
    public bool CreateCheckpointContainers { get; set; }

    /// <summary>Where events that fail permanently are copied before being checkpointed past; null only logs and skips them.</summary>
    public string? DeadLetterEventHub { get; set; }

    /// <summary>First delay before an event whose handler failed is retried; doubles up to <see cref="MaxRetryDelay"/>.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Longest wait before a failed event is retried; only its partition waits meanwhile, the others keep flowing.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Most events of one partition handed to batch handlers at once; a failed batch is retried event by event.</summary>
    public int MaxBatchSize { get; set; } = 1;

    internal IReadOnlyList<EventHubsListener> Listeners => _listeners;

    /// <summary>Consumes <paramref name="eventHub"/> as <paramref name="consumerGroup"/>, checkpointing to the blob container at <paramref name="checkpointContainerUri"/>.</summary>
    /// <remarks>The container is reached with <see cref="Credential"/> when set, otherwise the URI must carry a SAS token.</remarks>
    public EventHubsOptions Listen(string eventHub, string consumerGroup, Uri checkpointContainerUri)
    {
        ArgumentNullException.ThrowIfNull(checkpointContainerUri);
        return Add(eventHub, consumerGroup, options => options.Credential is { } credential
            ? new BlobContainerClient(checkpointContainerUri, credential)
            : new BlobContainerClient(checkpointContainerUri));
    }

    /// <summary>Consumes <paramref name="eventHub"/> as <paramref name="consumerGroup"/>, checkpointing to a container of the given storage account.</summary>
    public EventHubsOptions Listen(string eventHub, string consumerGroup, string storageConnectionString, string checkpointContainerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storageConnectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointContainerName);
        return Add(eventHub, consumerGroup, _ => new BlobContainerClient(storageConnectionString, checkpointContainerName));
    }

    /// <summary>Consumes <paramref name="eventHub"/> as <paramref name="consumerGroup"/>, checkpointing to <paramref name="checkpointContainer"/>.</summary>
    public EventHubsOptions Listen(string eventHub, string consumerGroup, BlobContainerClient checkpointContainer)
    {
        ArgumentNullException.ThrowIfNull(checkpointContainer);
        return Add(eventHub, consumerGroup, _ => checkpointContainer);
    }

    private EventHubsOptions Add(string eventHub, string consumerGroup, Func<EventHubsOptions, BlobContainerClient> checkpointContainer)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventHub);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        _listeners.Add(new EventHubsListener(eventHub, consumerGroup, checkpointContainer));
        return this;
    }
}

internal sealed record EventHubsListener(string EventHub, string ConsumerGroup, Func<EventHubsOptions, BlobContainerClient> CheckpointContainer);
