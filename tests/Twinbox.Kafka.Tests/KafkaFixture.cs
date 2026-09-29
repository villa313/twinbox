using Confluent.Kafka;
using Testcontainers.Kafka;

namespace Twinbox.Kafka.Tests;

public sealed class KafkaFixture : IAsyncLifetime
{
    private readonly KafkaContainer _container = new KafkaBuilder("apache/kafka:3.9.1").Build();

    public string BootstrapServers => _container.GetBootstrapAddress();

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public ValueTask DisposeAsync() => _container.DisposeAsync();

    /// <summary>The group's committed offset for partition 0, or <see cref="Offset.Unset"/> when nothing is committed.</summary>
    public Offset CommittedOffset(string groupId, string topic)
    {
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(
            new ConsumerConfig { BootstrapServers = BootstrapServers, GroupId = groupId, EnableAutoCommit = false }).Build();
        return consumer.Committed([new TopicPartition(topic, 0)], TimeSpan.FromSeconds(10)).Single().Offset;
    }

    public ConsumeResult<string?, byte[]> ConsumeOne(string topic, TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<string?, byte[]>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"inspector-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        consumer.Subscribe(topic);
        try
        {
            using var cts = new CancellationTokenSource(timeout);
            return consumer.Consume(cts.Token);
        }
        finally
        {
            consumer.Close();
        }
    }
}
