using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Benchmarks.Micro;

/// <summary>What IOutbox.Send costs the caller: route lookup, serialization and building the outbox rows.</summary>
[MemoryDiagnoser]
public class SendBenchmarks
{
    private static readonly SendOptions PartitionKeyOptions = new() { PartitionKey = "customer-42" };

    private static readonly SendOptions HeaderOptions = new()
    {
        PartitionKey = "customer-42",
        Headers = new Dictionary<string, string>
        {
            ["tenant-region"] = "eu-west",
            ["source-system"] = "checkout",
            ["schema-version"] = "3",
        },
    };

    private ServiceProvider _services = null!;
    private AsyncServiceScope _scope;
    private IOutbox _outbox = null!;
    private IOutboxSession _session = null!;
    private IMessageSerializer _serializer = null!;
    private object _message = null!;
    private Type _messageType = null!;

    [Params("Small", "Medium")]
    public string Payload { get; set; } = "Small";

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton<ITransport, NoOpTransport>();
        services.AddTwinbox(twinbox => twinbox
            .Route<OrderPlaced>().To("orders")
            .Route<OrderSubmitted>().To("orders"));
        _services = services.BuildServiceProvider();
        _scope = _services.CreateAsyncScope();
        _outbox = _scope.ServiceProvider.GetRequiredService<IOutbox>();
        _session = _scope.ServiceProvider.GetRequiredService<IOutboxSession>();
        _serializer = _services.GetRequiredService<IMessageSerializer>();
        _message = Payload == "Small" ? SampleMessages.Small() : SampleMessages.Medium();
        _messageType = _message.GetType();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _scope.Dispose();
        _services.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Serialize only")]
    public byte[] SerializeOnly() => _serializer.Serialize(_message, _messageType);

    [Benchmark(Description = "Send")]
    public IReadOnlyList<OutboxMessage> Send()
    {
        _outbox.Send(_message);
        return _session.TakePending();
    }

    [Benchmark(Description = "Send + partition key")]
    public IReadOnlyList<OutboxMessage> SendWithPartitionKey()
    {
        _outbox.Send(_message, PartitionKeyOptions);
        return _session.TakePending();
    }

    [Benchmark(Description = "Send + partition key + 3 headers")]
    public IReadOnlyList<OutboxMessage> SendWithHeaders()
    {
        _outbox.Send(_message, HeaderOptions);
        return _session.TakePending();
    }
}
