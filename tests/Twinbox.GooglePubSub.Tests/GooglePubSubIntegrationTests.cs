using System.Text;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.GooglePubSub.Tests;

[Trait("Category", "Integration")]
public sealed class GooglePubSubIntegrationTests(PubSubFixture pubSub) : IClassFixture<PubSubFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task OutboxMessage_IsConsumedByItsHandlerOnce()
    {
        var (topic, subscription) = (Name("orders"), Name("billing"));
        var journal = new Journal();
        await using var host = await GooglePubSubTestHost.StartAsync(
            pubSub,
            journal,
            new FailureGate(),
            b => b.Route<OrderPlaced>().To(topic).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(subscription, topic));

        await host.SendAsync(new OrderPlaced(1));
        await journal.WaitForAsync(1, Timeout);
        await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        Assert.Equal([1], journal.Handled);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Outbox.Snapshot()).Status);
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInbox()
    {
        var (topic, subscription) = (Name("orders"), Name("billing"));
        var journal = new Journal();
        await using var host = await GooglePubSubTestHost.StartAsync(
            pubSub,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(subscription, topic));
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(Message("dup-1", topic, 1, null), cancellation);
        await host.Transport.SendAsync(Message("dup-1", topic, 1, null), cancellation);
        await host.Transport.SendAsync(Message("after-dup", topic, 2, null), cancellation);
        await journal.WaitForAsync(1, Timeout);
        await journal.WaitForAsync(2, Timeout);
        await Task.Delay(TimeSpan.FromSeconds(1), cancellation);

        Assert.Equal([1, 2], journal.Handled.Order());
    }

    [Fact]
    public async Task OrderingKey_KeepsEachKeyInOrderAcrossRetries()
    {
        var (topic, subscription) = (Name("orders"), Name("billing"));
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(4);
        await using var host = await GooglePubSubTestHost.StartAsync(
            pubSub,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.EnableMessageOrdering = true;
                o.Listen(subscription, topic);
            });
        var cancellation = TestContext.Current.CancellationToken;

        for (var orderId = 1; orderId <= 30; orderId++)
        {
            await host.Transport.SendAsync(Message($"order-{orderId}", topic, orderId, $"customer-{orderId % 3}"), cancellation);
        }

        await gate.WaitForFailuresAsync(2, Timeout);
        gate.Release(4);
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
    public async Task PermanentHandlerFailure_IsCopiedToTheDeadLetterTopicAndAcknowledged()
    {
        var (topic, subscription) = (Name("orders"), Name("billing"));
        var (deadLetterTopic, deadLetters) = (topic + "-dlq", Name("dead-letters"));
        await pubSub.CreateTopicAsync(deadLetterTopic);
        await pubSub.CreateSubscriptionAsync(deadLetters, deadLetterTopic);
        await using var host = await GooglePubSubTestHost.StartAsync(
            pubSub,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o =>
            {
                o.DeadLetterTopic = deadLetterTopic;
                o.Listen(subscription, topic);
            });

        await host.Transport.SendAsync(Message("poison-1", topic, 3, "customer-3"), TestContext.Current.CancellationToken);

        var copy = GooglePubSubMapping.ToIncoming(await pubSub.PullOneAsync(deadLetters, Timeout), deadLetters);
        Assert.Equal("poison-1", copy.MessageId);
        Assert.Equal("order-placed", copy.MessageName);
        Assert.Equal("customer-3", copy.PartitionKey);
        Assert.Equal("application/json", copy.ContentType);
        Assert.Equal("""{"orderId":3}""", Encoding.UTF8.GetString(copy.Body.Span));
        Assert.Contains("can never be handled", copy.Headers[GooglePubSubMapping.ErrorAttribute], StringComparison.Ordinal);
        Assert.Equal(subscription, copy.Headers[GooglePubSubMapping.OriginAttribute]);
    }

    [Fact]
    public async Task TransientHandlerFailure_IsRedelivered()
    {
        var (topic, subscription) = (Name("orders"), Name("billing"));
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(5);
        await using var host = await GooglePubSubTestHost.StartAsync(
            pubSub,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(subscription, topic));

        await host.Transport.SendAsync(Message("flaky-5", topic, 5, null), TestContext.Current.CancellationToken);
        await gate.WaitForFailuresAsync(2, Timeout);

        Assert.Empty(journal.Handled);

        gate.Release(5);
        await journal.WaitForAsync(5, Timeout);

        Assert.Equal([5], journal.Handled);
    }

    [Fact]
    public async Task MissingTopic_IsPermanentFailure()
    {
        await using var host = await GooglePubSubTestHost.StartAsync(pubSub, new Journal(), new FailureGate(), _ => { }, o => o.AutoCreate = false);

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(Message("lost-1", Name("missing"), 1, null), TestContext.Current.CancellationToken));

        Assert.Contains("Pub/Sub rejected", error.Message, StringComparison.Ordinal);
    }

    private static string Name(string prefix) => $"{prefix}-{Guid.NewGuid().ToString("N")[..8]}";

    private static TransportMessage Message(string id, string destination, int orderId, string? partitionKey) => new(
        id,
        "order-placed",
        destination,
        Encoding.UTF8.GetBytes($$"""{"orderId":{{orderId}}}"""),
        "application/json",
        new Dictionary<string, string>(),
        partitionKey);
}
