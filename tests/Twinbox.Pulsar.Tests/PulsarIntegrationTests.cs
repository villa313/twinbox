using System.Buffers;
using System.Text;
using DotPulsar;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Pulsar.Tests;

[Trait("Category", "Integration")]
public sealed class PulsarIntegrationTests(PulsarFixture broker) : IClassFixture<PulsarFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task OutboxMessage_IsConsumedByItsHandlerOnceAndAcknowledged()
    {
        var (topic, subscription) = Names();
        var journal = new Journal();
        await using var host = await PulsarTestHost.StartAsync(
            broker.ServiceUrl,
            journal,
            new FailureGate(),
            b => b.Route<OrderPlaced>().To(topic).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(topic, subscription));

        await host.SendAsync(new OrderPlaced(1));
        await journal.WaitForAsync(1, Timeout);
        await broker.WaitForEmptyBacklogAsync(topic, subscription, Timeout);
        await Task.Delay(300, TestContext.Current.CancellationToken);

        var handled = Assert.Single(journal.Entries);
        Assert.Equal(1, handled.OrderId);
        Assert.Equal(1, handled.DeliveryAttempt);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Outbox.Snapshot()).Status);
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInbox()
    {
        var (topic, subscription) = Names();
        var journal = new Journal();
        await using var host = await PulsarTestHost.StartAsync(
            broker.ServiceUrl,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(topic, subscription));
        var duplicate = Message("dup-1", topic, 1, "customer-1");
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(Message("after-dup", topic, 2, "customer-1"), cancellation);
        await journal.WaitForAsync(2, Timeout);
        await broker.WaitForEmptyBacklogAsync(topic, subscription, Timeout);

        // One consumer receives the topic in order, so the sentinel arriving means both copies were processed.
        Assert.Equal([1, 2], journal.Handled);
        Assert.Equal("customer-1", journal.Entries[0].PartitionKey);
    }

    [Fact]
    public async Task MessagesWithTheSamePartitionKey_AreHandledInOrderByKeySharedConsumers()
    {
        var (topic, subscription) = Names();
        var journal = new Journal();
        await using var host = await PulsarTestHost.StartAsync(
            broker.ServiceUrl,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.SubscriptionType = SubscriptionType.KeyShared;
                o.ConsumerConcurrency = 3;
                o.Listen(topic, subscription);
            });
        var cancellation = TestContext.Current.CancellationToken;

        for (var orderId = 1; orderId <= 60; orderId++)
        {
            await host.Transport.SendAsync(Message($"order-{orderId}", topic, orderId, $"customer-{orderId % 4}"), cancellation);
        }

        using var cts = new CancellationTokenSource(Timeout);
        while (journal.Handled.Count < 60)
        {
            await Task.Delay(50, cts.Token);
        }

        foreach (var perKey in journal.Entries.GroupBy(e => e.PartitionKey))
        {
            var orders = perKey.Select(e => e.OrderId).ToArray();
            Assert.Equal(orders.Order(), orders);
            Assert.Equal(15, orders.Length);
        }
    }

    [Fact]
    public async Task PermanentHandlerFailure_IsCopiedToTheDeadLetterTopicAndAcknowledged()
    {
        var (topic, subscription) = Names();
        await using var host = await PulsarTestHost.StartAsync(
            broker.ServiceUrl,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o => o.Listen(topic, subscription));

        await host.Transport.SendAsync(Message("poison-1", topic, 3, "customer-3"), TestContext.Current.CancellationToken);

        var dead = Assert.Single(await broker.ReadAsync($"{topic}-dlq", 1, Timeout));
        Assert.Equal("customer-3", dead.Key);
        Assert.Equal("poison-1", dead.Properties[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", dead.Properties[TransportHeaders.MessageName]);
        Assert.Equal("""{"orderId":3}""", Encoding.UTF8.GetString(dead.Data.ToArray()));
        Assert.Contains("can never be handled", dead.Properties[PulsarMapping.ErrorProperty], StringComparison.Ordinal);
        Assert.StartsWith($"{topic}:", dead.Properties[PulsarMapping.OriginProperty], StringComparison.Ordinal);
        Assert.Equal(subscription, dead.Properties[PulsarMapping.SubscriptionProperty]);
        await broker.WaitForEmptyBacklogAsync(topic, subscription, Timeout);
    }

    [Fact]
    public async Task TransientHandlerFailure_IsRedeliveredWithTheNextAttempt()
    {
        var (topic, subscription) = Names();
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(5, times: 1);
        await using var host = await PulsarTestHost.StartAsync(
            broker.ServiceUrl,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(topic, subscription));

        await host.Transport.SendAsync(Message("flaky-5", topic, 5, "customer-5"), TestContext.Current.CancellationToken);
        await journal.WaitForAsync(5, Timeout);
        await broker.WaitForEmptyBacklogAsync(topic, subscription, Timeout);

        Assert.Equal(1, gate.Failures);
        Assert.Equal(2, Assert.Single(journal.Entries).DeliveryAttempt);
    }

    [Fact]
    public async Task MessageFailingEveryDelivery_IsDeadLetteredAfterMaxRedeliveryCount()
    {
        var (topic, subscription) = Names();
        var gate = new FailureGate();
        gate.Hold(8);
        await using var host = await PulsarTestHost.StartAsync(
            broker.ServiceUrl,
            new Journal(),
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.MaxRedeliveryCount = 2;
                o.NegativeAckRedeliveryDelay = TimeSpan.FromMilliseconds(100);
                o.Listen(topic, subscription);
            });

        await host.Transport.SendAsync(Message("stuck-8", topic, 8, null), TestContext.Current.CancellationToken);

        var dead = Assert.Single(await broker.ReadAsync($"{topic}-dlq", 1, Timeout));
        Assert.Equal("InvalidOperationException: Order 8 is held.", dead.Properties[PulsarMapping.ErrorProperty]);
        await broker.WaitForEmptyBacklogAsync(topic, subscription, Timeout);
        Assert.Equal(3, gate.Failures);
    }

    [Fact]
    public async Task OversizedMessage_IsPermanentFailureAndLeavesTheTopicUsable()
    {
        var (topic, subscription) = Names();
        var journal = new Journal();
        await using var host = await PulsarTestHost.StartAsync(
            broker.ServiceUrl,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(topic, subscription));
        var cancellation = TestContext.Current.CancellationToken;
        var oversized = new TransportMessage(
            "huge-1", "order-placed", topic, new byte[6 * 1024 * 1024], "application/json", new Dictionary<string, string>(), null);

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(() => host.Transport.SendAsync(oversized, cancellation));
        await host.Transport.SendAsync(Message("after-huge", topic, 9, null), cancellation);
        await journal.WaitForAsync(9, Timeout);

        Assert.Contains(topic, error.Message, StringComparison.Ordinal);
        Assert.Equal([9], journal.Handled);
    }

    private static (string Topic, string Subscription) Names()
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
}
