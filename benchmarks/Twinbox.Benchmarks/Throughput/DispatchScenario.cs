using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Serialization;
using Twinbox.Storage;

namespace Twinbox.Benchmarks.Throughput;

/// <summary>Drain rate of a pre-filled outbox; each dispatcher gets its own instance id, as separate processes would.</summary>
internal static class DispatchScenario
{
    private const int SeedChunk = 1000;

    public static async Task<Measurement> RunAsync(BenchStore store, int batchSize, int dispatchers, int messages, int runs)
    {
        var providers = Enumerable.Range(0, dispatchers)
            .Select(i => store.Build(twinbox => twinbox.Configure(o =>
            {
                o.InstanceId = $"bench-dispatcher-{i}";
                o.Dispatcher.BatchSize = batchSize;
            })))
            .ToArray();
        try
        {
            var outbox = providers[0].GetServices<IOutboxStore>().Single();
            var payload = providers[0].GetRequiredService<IMessageSerializer>().Serialize(SampleMessages.Small(), typeof(OrderPlaced));
            return await Measurement.TakeAsync(
                runs,
                async () =>
                {
                    await store.ResetAsync();
                    await SeedAsync(outbox, payload, messages);
                    await store.AnalyzeAsync();
                },
                async () =>
                {
                    var stopwatch = Stopwatch.StartNew();
                    await Task.WhenAll(providers.Select(p => Task.Run(() => DrainAsync(p.GetRequiredService<IOutboxDispatcher>()))));
                    var elapsed = stopwatch.Elapsed;

                    var statistics = await outbox.GetStatisticsAsync(default);
                    if (statistics.PendingCount != 0 || statistics.DeadCount != 0)
                    {
                        throw new InvalidOperationException(
                            $"Drain left {statistics.PendingCount} pending and {statistics.DeadCount} dead messages.");
                    }

                    return messages / elapsed.TotalSeconds;
                });
        }
        finally
        {
            foreach (var provider in providers)
            {
                await provider.DisposeAsync();
            }
        }
    }

    private static async Task DrainAsync(IOutboxDispatcher dispatcher)
    {
        // Zero means every remaining message is leased by another dispatcher.
        while (await dispatcher.DispatchBatchAsync(default) > 0)
        {
        }
    }

    private static async Task SeedAsync(IOutboxStore outbox, byte[] payload, int messages)
    {
        var generator = new Uuid7MessageIdGenerator();
        var now = DateTimeOffset.UtcNow.AddSeconds(-1);
        for (var offset = 0; offset < messages; offset += SeedChunk)
        {
            var chunk = Enumerable.Range(0, Math.Min(SeedChunk, messages - offset))
                .Select(_ => new OutboxMessage
                {
                    Id = generator.NewId(now),
                    MessageName = nameof(OrderPlaced),
                    Transport = NoOpTransport.TransportName,
                    Destination = "orders",
                    Payload = payload,
                    ContentType = "application/json",
                    CreatedAt = now,
                    AvailableAt = now,
                    Status = OutboxMessageStatus.Pending,
                })
                .ToArray();
            await outbox.AppendAsync(chunk, default);
        }
    }
}
