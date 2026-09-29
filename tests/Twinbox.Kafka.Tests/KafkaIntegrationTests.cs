using System.Text;
using Confluent.Kafka;
using Twinbox.Storage;
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
