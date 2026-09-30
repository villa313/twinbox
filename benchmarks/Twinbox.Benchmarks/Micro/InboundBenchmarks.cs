using System.Globalization;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.InMemory;
using Twinbox.Serialization;
using Twinbox.Transport;

namespace Twinbox.Benchmarks.Micro;

/// <summary>Per-message cost of the inbound pipeline with a no-op handler: deserialize, scope, dispatch, dedup.</summary>
[MemoryDiagnoser]
public class InboundBenchmarks
{
    // Batches keep invocations long enough to time without per-iteration setup, which BenchmarkDotNet runs once per call.
    private const int Messages = 1000;

    private static readonly Dictionary<string, string> Headers = new()
    {
        [TransportHeaders.TraceParent] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    };

    private ServiceProvider _withInbox = null!;
    private ServiceProvider _withoutInbox = null!;
    private IInboundPipeline _inboxPipeline = null!;
    private IInboundPipeline _plainPipeline = null!;
    private IMessageSerializer _serializer = null!;
    private InMemoryInboxStore _inbox = null!;
    private byte[] _body = null!;
    private IncomingMessage _duplicate = null!;
    private long _next;

    [GlobalSetup]
    public void Setup()
    {
        _withInbox = Build(inbox: true);
        _withoutInbox = Build(inbox: false);
        _inboxPipeline = _withInbox.GetRequiredService<IInboundPipeline>();
        _plainPipeline = _withoutInbox.GetRequiredService<IInboundPipeline>();
        _inbox = _withInbox.GetRequiredService<InMemoryInboxStore>();
        _serializer = _withInbox.GetRequiredService<IMessageSerializer>();
        _body = _serializer.Serialize(SampleMessages.Small(), typeof(OrderPlaced));
        _duplicate = Message("already-processed");
        _inboxPipeline.ProcessAsync(_duplicate, default).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _withInbox.Dispose();
        _withoutInbox.Dispose();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Messages, Description = "Deserialize only")]
    public object? DeserializeOnly()
    {
        object? body = null;
        for (var i = 0; i < Messages; i++)
        {
            body = _serializer.Deserialize(_body, typeof(OrderPlaced));
        }

        return body;
    }

    [Benchmark(OperationsPerInvoke = Messages, Description = "Pipeline, inbox off")]
    public async Task InboxOff()
    {
        for (var i = 0; i < Messages; i++)
        {
            await _plainPipeline.ProcessAsync(Message(NextId()), default);
        }
    }

    /// <summary>Includes purging the batch's entries afterwards, as retention would, so the store doesn't grow without bound.</summary>
    [Benchmark(OperationsPerInvoke = Messages, Description = "Pipeline, in-memory inbox")]
    public async Task InboxOn()
    {
        for (var i = 0; i < Messages; i++)
        {
            await _inboxPipeline.ProcessAsync(Message(NextId()), default);
        }

        await _inbox.PurgeAsync(DateTimeOffset.MaxValue, Messages, default);
    }

    [Benchmark(OperationsPerInvoke = Messages, Description = "Pipeline, in-memory inbox, duplicate")]
    public async Task Duplicate()
    {
        for (var i = 0; i < Messages; i++)
        {
            await _inboxPipeline.ProcessAsync(_duplicate, default);
        }
    }

    private static ServiceProvider Build(bool inbox)
    {
        var services = new ServiceCollection().AddLogging();
        services.AddTwinbox(twinbox => twinbox
            .UseInMemoryStore()
            .AddHandler<NoOpHandler, OrderPlaced>()
            .Configure(o => o.Inbox.Enabled = inbox));
        return services.BuildServiceProvider();
    }

    private string NextId() => (++_next).ToString(CultureInfo.InvariantCulture);

    private IncomingMessage Message(string id) =>
        new(id, nameof(OrderPlaced), "orders", _body, "application/json", Headers, 1, null);
}
