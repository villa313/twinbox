using System.Text;
using Confluent.Kafka;
using Twinbox.Storage;
using Twinbox.Tests.Shared;
using Twinbox.Transport;

namespace Twinbox.Kafka.Tests;

[Trait("Category", "Integration")]
public sealed class KafkaIntegrationTests(KafkaFixture broker) : IClassFixture<KafkaFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task OutboxMessage_IsConsumedByItsHandlerOnce()
    {
        var (topic, group) = Names();
        var journal = new Journal();
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            journal,
            new FailureGate(),
            b => b.Route<OrderPlaced>().To(topic).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(topic, group));

        await host.SendAsync(new OrderPlaced(1));
        await journal.WaitForAsync(1, Timeout);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.Equal([1], journal.Handled);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Outbox.Snapshot()).Status);
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInbox()
    {
        var (topic, group) = Names();
        var journal = new Journal();
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(topic, group));
        var duplicate = Message("dup-1", topic, 1, "customer-1");
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(Message("after-dup", topic, 2, "customer-1"), cancellation);
        await journal.WaitForAsync(2, Timeout);

        // The same key lands on one partition, read in order, so the sentinel arriving means both copies were processed.
        Assert.Equal([1, 2], journal.Handled);
    }

    [Fact]
    public async Task MessagesWithTheSamePartitionKey_AreHandledInOrder()
    {
        var (topic, group) = Names();
        var journal = new Journal();
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.TopicPartitions = 3;
                o.Listen(topic, group);
            });
        var cancellation = TestContext.Current.CancellationToken;

        for (var orderId = 1; orderId <= 30; orderId++)
        {
            await host.Transport.SendAsync(Message($"order-{orderId}", topic, orderId, $"customer-{orderId % 3}"), cancellation);
        }

        using var cts = new CancellationTokenSource(Timeout);
        while (journal.Handled.Count < 30)
        {
            await Task.Delay(50, cts.Token);
        }

        foreach (var perKey in journal.Entries.GroupBy(e => e.PartitionKey))
        {
            var orders = perKey.Select(e => e.OrderId).ToArray();
            Assert.Equal(orders.Order(), orders);
            Assert.Equal(10, orders.Length);
        }
    }

    [Fact]
    public async Task PermanentHandlerFailure_IsCopiedToTheDeadLetterTopicAndSkipped()
    {
        var (topic, group) = Names();
        var deadLetterTopic = $"{topic}.dlq";
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o =>
            {
                o.DeadLetterTopic = deadLetterTopic;
                o.Listen(topic, group);
            });

        await host.Transport.SendAsync(Message("poison-1", topic, 3, "customer-3"), TestContext.Current.CancellationToken);

        var deadLettered = broker.ConsumeOne(deadLetterTopic, Timeout);
        Assert.Equal("customer-3", deadLettered.Message.Key);
        Assert.Equal("poison-1", Header(deadLettered, TransportHeaders.MessageId));
        Assert.Equal("order-placed", Header(deadLettered, TransportHeaders.MessageName));
        Assert.Contains("can never be handled", Header(deadLettered, KafkaMapping.ErrorHeader), StringComparison.Ordinal);
        Assert.Equal($"{topic}:0:0", Header(deadLettered, KafkaMapping.OriginHeader));
        await WaitForCommitAsync(group, topic, 1);
    }

    [Fact]
    public async Task PermanentHandlerFailureWithoutADeadLetterTopic_IsCountedAndSkipped()
    {
        var (topic, group) = Names();
        using var discarded = new CounterProbe("twinbox.inbox.discarded", topic);
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o => o.Listen(topic, group));

        await host.Transport.SendAsync(Message("poison-2", topic, 4, "customer-4"), TestContext.Current.CancellationToken);

        await WaitForCommitAsync(group, topic, 1);
        Assert.Equal(1, discarded.Value);
    }

    [Fact]
    public async Task TransientHandlerFailure_IsRetriedWithoutCommittingPastIt()
    {
        var (topic, group) = Names();
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(5);
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(topic, group));
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(Message("flaky-5", topic, 5, "customer-5"), cancellation);
        await host.Transport.SendAsync(Message("after-flaky", topic, 6, "customer-5"), cancellation);
        await gate.WaitForFailuresAsync(3, Timeout);

        Assert.Empty(journal.Handled);
        Assert.Equal(Offset.Unset, broker.CommittedOffset(group, topic));

        gate.Release(5);
        await journal.WaitForAsync(6, Timeout);

        Assert.Equal([5, 6], journal.Handled);
        await WaitForCommitAsync(group, topic, 2);
    }

    [Fact]
    public async Task TransientHandlerFailure_PausesOnlyItsPartition()
    {
        var (topic, group) = Names();
        await broker.CreateTopicAsync(topic, 2);
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(1);
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                // Far longer than the other partition needs, so it can only finish while partition 0 waits.
                o.RetryDelay = TimeSpan.FromSeconds(10);
                o.MaxRetryDelay = TimeSpan.FromSeconds(10);
                o.Listen(topic, group);
            });

        await broker.ProduceAsync(topic, 0, Records(topic, 1, 2, 3));
        await gate.WaitForFailuresAsync(1, Timeout);
        await broker.ProduceAsync(topic, 1, Records(topic, 11, 12, 13, 14, 15));
        await journal.WaitForAsync(15, TimeSpan.FromSeconds(6));

        Assert.Equal([11, 12, 13, 14, 15], journal.Handled);

        gate.Release(1);
        await journal.WaitForAsync(3, Timeout);

        Assert.Equal([1, 2, 3], journal.Handled.Where(id => id < 10));
        Assert.Equal(1, gate.Failures);
    }

    [Fact]
    public async Task BatchHandler_ReceivesRecordsOfAPartitionTogether()
    {
        var (topic, group) = Names();
        await broker.CreateTopicAsync(topic, 1);
        await broker.ProduceAsync(topic, 0, Records(topic, [.. Enumerable.Range(1, 30)]));
        var journal = new Journal();
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            journal,
            new FailureGate(),
            b => b.AddBatchHandler<RecordingBatchHandler, OrderPlaced>(),
            o =>
            {
                o.MaxBatchSize = 10;
                o.Listen(topic, group);
            });

        await journal.WaitForAsync(30, Timeout);
        await WaitForCommitAsync(group, topic, 30);

        Assert.Equal(Enumerable.Range(1, 30), journal.Handled);
        Assert.Contains(host.BatchLog.Sizes, size => size > 1);
        Assert.All(host.BatchLog.Sizes, size => Assert.InRange(size, 1, 10));
    }

    [Fact]
    public async Task BatchWithAPermanentFailure_DeadLettersOnlyTheBadRecord()
    {
        var (topic, group) = Names();
        var deadLetterTopic = $"{topic}.dlq";
        await broker.CreateTopicAsync(topic, 1);
        await broker.ProduceAsync(topic, 0, Records(topic, 97, 98, 99, 100, 101));
        var journal = new Journal();
        await using var host = await KafkaTestHost.StartAsync(
            broker.BootstrapServers,
            journal,
            new FailureGate(),
            b => b.AddBatchHandler<RecordingBatchHandler, OrderPlaced>(),
            o =>
            {
                o.MaxBatchSize = 10;
                o.DeadLetterTopic = deadLetterTopic;
                o.Listen(topic, group);
            });

        var deadLettered = broker.ConsumeOne(deadLetterTopic, Timeout);
        await WaitForCommitAsync(group, topic, 5);

        Assert.Equal($"order-{RecordingBatchHandler.Poison}", Header(deadLettered, TransportHeaders.MessageId));
        Assert.Equal([97, 98, 100, 101], journal.Handled);
    }

    [Fact]
    public async Task OversizedMessage_IsPermanentFailure()
    {
        var (topic, _) = Names();
        await using var host = await KafkaTestHost.StartAsync(broker.BootstrapServers, new Journal(), new FailureGate(), _ => { });
        var oversized = new TransportMessage(
            "huge-1", "order-placed", topic, new byte[2 * 1024 * 1024], "application/json", new Dictionary<string, string>(), null);

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(oversized, TestContext.Current.CancellationToken));

        Assert.Contains(topic, error.Message, StringComparison.Ordinal);
    }

    private static (string Topic, string Group) Names()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return ($"orders-{suffix}", $"billing-{suffix}");
    }

    private static TransportMessage Message(string id, string topic, int orderId, string? partitionKey) => new(
        id,
        "order-placed",
        topic,
        Encoding.UTF8.GetBytes($$"""{"orderId":{{orderId}}}"""),
        "application/json",
        new Dictionary<string, string>(),
        partitionKey);

    private static IEnumerable<Message<string?, byte[]>> Records(string topic, params int[] orderIds) =>
        orderIds.Select(id => KafkaMapping.ToKafkaMessage(Message($"order-{id}", topic, id, $"customer-{id}")));

    private static string Header(ConsumeResult<string?, byte[]> record, string name) =>
        Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes(name));

    private async Task WaitForCommitAsync(string group, string topic, long offset)
    {
        using var cts = new CancellationTokenSource(Timeout);
        while (broker.CommittedOffset(group, topic).Value != offset)
        {
            await Task.Delay(100, cts.Token);
        }
    }
}
