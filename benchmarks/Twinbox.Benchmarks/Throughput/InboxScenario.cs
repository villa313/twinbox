using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Serialization;
using Twinbox.Transport;

namespace Twinbox.Benchmarks.Throughput;

internal sealed record InboxResult(Measurement InboxOff, Measurement InboxOn, Measurement Duplicate);

/// <summary>Milliseconds per message, processed one at a time by a handler that inserts one row.</summary>
internal static class InboxScenario
{
    public static async Task<InboxResult> RunAsync(BenchStore store, int messages, int runs)
    {
        var off = await MeasureAsync(store, inbox: false, redeliver: false, messages, runs);
        var on = await MeasureAsync(store, inbox: true, redeliver: false, messages, runs);
        var duplicate = await MeasureAsync(store, inbox: true, redeliver: true, messages, runs);
        return new InboxResult(off, on, duplicate);
    }

    private static async Task<Measurement> MeasureAsync(BenchStore store, bool inbox, bool redeliver, int count, int runs)
    {
        await using var services = store.Build(twinbox => twinbox.Configure(o => o.Inbox.Enabled = inbox));
        var pipeline = services.GetRequiredService<IInboundPipeline>();
        var body = services.GetRequiredService<IMessageSerializer>().Serialize(SampleMessages.Small(), typeof(OrderPlaced));
        var messages = Enumerable.Range(0, count)
            .Select(i => new IncomingMessage(
                $"message-{i.ToString(CultureInfo.InvariantCulture)}",
                nameof(OrderPlaced),
                "orders",
                body,
                "application/json",
                new Dictionary<string, string>(),
                1,
                null))
            .ToArray();

        return await Measurement.TakeAsync(
            runs,
            async () =>
            {
                await store.ResetAsync();
                if (redeliver)
                {
                    await ProcessAllAsync(pipeline, messages);
                }
            },
            async () =>
            {
                var stopwatch = Stopwatch.StartNew();
                await ProcessAllAsync(pipeline, messages);
                return stopwatch.Elapsed.TotalMilliseconds / count;
            });
    }

    private static async Task ProcessAllAsync(IInboundPipeline pipeline, IncomingMessage[] messages)
    {
        foreach (var message in messages)
        {
            await pipeline.ProcessAsync(message, default);
        }
    }
}
