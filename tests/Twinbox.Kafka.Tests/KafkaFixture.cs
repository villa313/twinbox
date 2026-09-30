using Confluent.Kafka;
using Confluent.Kafka.Admin;
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

    public async Task CreateTopicAsync(string topic, int partitions)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }]);
    }

    /// <summary>Writes straight to one partition, so a test controls placement and can queue records before a consumer starts.</summary>
    public async Task ProduceAsync(string topic, int partition, IEnumerable<Message<string?, byte[]>> messages)
    {
        using var producer = new ProducerBuilder<string?, byte[]>(new ProducerConfig { BootstrapServers = BootstrapServers }).Build();
        foreach (var message in messages)
        {
            await producer.ProduceAsync(new TopicPartition(topic, partition), message);
        }
    }
}
