using System.Runtime.CompilerServices;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.Azurite;
using Testcontainers.EventHubs;

namespace Twinbox.EventHubs.Tests.Integration;

/// <summary>The emulator cannot create event hubs at runtime, so each test gets its own from this fixed set.</summary>
public static class Hubs
{
    public const string Once = "orders-once";
    public const string Duplicates = "orders-duplicates";
    public const string Ordered = "orders-ordered";
    public const string Poison = "orders-poison";
    public const string Flaky = "orders-flaky";
    public const string Batched = "orders-batched";
    public const string BatchedPoison = "orders-batched-poison";
    public const string SendOnly = "orders-send-only";
    public const string DeadLetters = "orders-dead-letters";
}

public sealed class EventHubsFixture : IAsyncLifetime
{
    private const string AzuriteAlias = "azurite";
    private const string ConsumerGroup = "$Default";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly AzuriteContainer _azurite;
    private readonly EventHubsContainer _eventHubs;

    public EventHubsFixture()
    {
        _azurite = new AzuriteBuilder("mcr.microsoft.com/azure-storage/azurite:3.35.0")
            .WithNetwork(_network)
            .WithNetworkAliases(AzuriteAlias)
            .WithInMemoryPersistence()
            .Build();

        // Emulator entities are declared in its JSON config, which this builder writes into the container.
        var configuration = EventHubsServiceConfiguration.Create()
            .WithEntity(Hubs.Once, 1, ConsumerGroup)
            .WithEntity(Hubs.Duplicates, 1, ConsumerGroup)
            .WithEntity(Hubs.Ordered, 4, ConsumerGroup)
            .WithEntity(Hubs.Poison, 1, ConsumerGroup)
            .WithEntity(Hubs.Flaky, 2, ConsumerGroup)
            .WithEntity(Hubs.Batched, 1, ConsumerGroup)
            .WithEntity(Hubs.BatchedPoison, 1, ConsumerGroup)
            .WithEntity(Hubs.SendOnly, 1, ConsumerGroup)
            .WithEntity(Hubs.DeadLetters, 1, ConsumerGroup);
        _eventHubs = new EventHubsBuilder("mcr.microsoft.com/azure-messaging/eventhubs-emulator:latest")
            .WithAcceptLicenseAgreement(true)
            .WithConfigurationBuilder(configuration)
            .WithAzuriteContainer(_network, _azurite, AzuriteAlias)
            .Build();
    }

    public string ConnectionString => _eventHubs.GetConnectionString();

    public string StorageConnectionString => _azurite.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync();
        await _azurite.StartAsync();
        await _eventHubs.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _eventHubs.DisposeAsync();
        await _azurite.DisposeAsync();
        await _network.DisposeAsync();
    }

    /// <summary>Writes straight to one partition, so a test controls placement and can queue events before a processor starts.</summary>
    public async Task SendToPartitionAsync(string eventHub, string partitionId, IEnumerable<EventData> events)
    {
        await using var producer = new EventHubProducerClient(ConnectionString, eventHub);
        await producer.SendAsync(events, new SendEventOptions { PartitionId = partitionId });
    }

    /// <summary>Reads the whole partition from the start until an event matches or the timeout passes.</summary>
    public async Task<EventData> ReadUntilAsync(string eventHub, Func<EventData, bool> match, TimeSpan timeout)
    {
        await using var consumer = new EventHubConsumerClient(ConsumerGroup, ConnectionString, eventHub);
        using var cts = new CancellationTokenSource(timeout);
        await foreach (var data in ReadAsync(consumer, cts.Token))
        {
            if (match(data))
            {
                return data;
            }
        }

        throw new TimeoutException($"No matching event arrived on {eventHub}.");
    }

    /// <summary>The sequence number checkpointed for the partition, or null before the first checkpoint.</summary>
    public async Task<long?> CheckpointAsync(string container, string eventHub, string partitionId)
    {
        var client = new BlobContainerClient(StorageConnectionString, container);
        if (!await client.ExistsAsync())
        {
            return null;
        }

        var suffix = $"/{eventHub}/{ConsumerGroup}/checkpoint/{partitionId}".ToLowerInvariant();
        await foreach (var blob in client.GetBlobsAsync(Azure.Storage.Blobs.Models.BlobTraits.Metadata))
        {
            if (blob.Name.EndsWith(suffix, StringComparison.Ordinal) && blob.Metadata.TryGetValue("sequencenumber", out var sequence))
            {
                return long.Parse(sequence, System.Globalization.CultureInfo.InvariantCulture);
            }
        }

        return null;
    }

    private static async IAsyncEnumerable<EventData> ReadAsync(EventHubConsumerClient consumer, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var options = new ReadEventOptions { MaximumWaitTime = TimeSpan.FromMilliseconds(500) };
        await foreach (var partitionEvent in consumer.ReadEventsFromPartitionAsync("0", EventPosition.Earliest, options, cancellationToken))
        {
            if (partitionEvent.Data is { } data)
            {
                yield return data;
            }
        }
    }
}
