using System.Text;
using Amazon.SQS.Model;
using Twinbox.Storage;
using Twinbox.Tests.Shared;
using Twinbox.Transport;

namespace Twinbox.AmazonSqs.Tests;

[Trait("Category", "Integration")]
public sealed class AmazonSqsIntegrationTests(LocalStackFixture localStack) : IClassFixture<LocalStackFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task OutboxMessage_IsConsumedByItsHandlerOnce()
    {
        var queue = Name("orders");
        var journal = new Journal();
        await using var host = await AmazonSqsTestHost.StartAsync(
            localStack,
            journal,
            new FailureGate(),
            b => b.Route<OrderPlaced>().To(queue).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(queue));

        await host.SendAsync(new OrderPlaced(1));
        await journal.WaitForAsync(1, Timeout);
        await WaitForEmptyAsync(queue);

        Assert.Equal([1], journal.Handled);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Outbox.Snapshot()).Status);
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInbox()
    {
        var queue = Name("orders");
        var journal = new Journal();
        await using var host = await AmazonSqsTestHost.StartAsync(
            localStack,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(queue));
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(Message("dup-1", queue, 1, null), cancellation);
        await host.Transport.SendAsync(Message("dup-1", queue, 1, null), cancellation);
        await host.Transport.SendAsync(Message("after-dup", queue, 2, null), cancellation);
        await journal.WaitForAsync(1, Timeout);
        await journal.WaitForAsync(2, Timeout);
        await WaitForEmptyAsync(queue);

        // Both copies were deleted, so the second one reached the inbox and was skipped there.
        Assert.Equal([1, 2], journal.Handled.Order());
    }

    [Fact]
    public async Task TopicDestination_FansOutToEverySubscribedQueue()
    {
        var topic = Name("order-events");
        var (billingQueue, shippingQueue) = (Name("billing"), Name("shipping"));
        var (billing, shipping) = (new Journal(), new Journal());
        await using var shippingHost = await AmazonSqsTestHost.StartAsync(
            localStack,
            shipping,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(shippingQueue, topic));
        await using var billingHost = await AmazonSqsTestHost.StartAsync(
            localStack,
            billing,
            new FailureGate(),
            b => b.Route<OrderPlaced>().To(AmazonSqsTransport.TopicPrefix + topic).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(billingQueue, topic));

        await billingHost.SendAsync(new OrderPlaced(7));
        await billing.WaitForAsync(7, Timeout);
        await shipping.WaitForAsync(7, Timeout);

        Assert.Equal([7], billing.Handled);
        Assert.Equal([7], shipping.Handled);
    }

    [Fact]
    public async Task FifoQueue_HandlesEachPartitionKeyInOrderAcrossRetries()
    {
        var queue = Name("orders") + ".fifo";
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(4);
        await using var host = await AmazonSqsTestHost.StartAsync(
            localStack,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.MaxConcurrency = 10;
                o.Listen(queue);
            });
        var cancellation = TestContext.Current.CancellationToken;

        for (var orderId = 1; orderId <= 30; orderId++)
        {
            await host.Transport.SendAsync(Message($"order-{orderId}", queue, orderId, $"customer-{orderId % 3}"), cancellation);
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
    public async Task PermanentHandlerFailure_IsCopiedToTheDeadLetterQueueAndDeleted()
    {
        var queue = Name("orders");
        var deadLetterQueue = queue + "-dlq";
        await using var host = await AmazonSqsTestHost.StartAsync(
            localStack,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o =>
            {
                o.DeadLetterQueue = deadLetterQueue;
                o.Listen(queue);
            });

        await host.Transport.SendAsync(Message("poison-1", queue, 3, "customer-3"), TestContext.Current.CancellationToken);

        var deadLettered = await localStack.ReceiveOneAsync(deadLetterQueue, Timeout);
        var copy = AmazonSqsMapping.ToIncoming(deadLettered, deadLetterQueue);
        Assert.Equal("poison-1", copy.MessageId);
        Assert.Equal("order-placed", copy.MessageName);
        Assert.Equal("customer-3", copy.PartitionKey);
        Assert.Equal("""{"orderId":3}""", Encoding.UTF8.GetString(copy.Body.Span));
        Assert.Contains("can never be handled", copy.Headers[AmazonSqsMapping.ErrorHeader], StringComparison.Ordinal);
        Assert.Equal(queue, copy.Headers[AmazonSqsMapping.OriginHeader]);
        await WaitForEmptyAsync(queue);
    }

    [Fact]
    public async Task PermanentHandlerFailureWithNowhereToDeadLetter_IsCountedAndDeleted()
    {
        var queue = Name("orders");
        using var discarded = new CounterProbe("twinbox.inbox.discarded", queue);
        await using var host = await AmazonSqsTestHost.StartAsync(
            localStack,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o => o.Listen(queue));

        await host.Transport.SendAsync(Message("poison-2", queue, 4, null), TestContext.Current.CancellationToken);

        await WaitForEmptyAsync(queue);
        Assert.Equal(1, discarded.Value);
    }

    [Fact]
    public async Task PermanentHandlerFailureOnAQueueWithARedrivePolicy_IsLeftToIt()
    {
        var queue = Name("orders");
        var redriveQueue = queue + "-redrive";
        var redriveUrl = await localStack.CreateQueueAsync(redriveQueue);
        var redriveArn = (await localStack.Sqs.GetQueueAttributesAsync(redriveUrl, ["QueueArn"])).QueueARN;
        await localStack.Sqs.CreateQueueAsync(new CreateQueueRequest
        {
            QueueName = queue,
            Attributes = new() { ["RedrivePolicy"] = $$"""{"deadLetterTargetArn":"{{redriveArn}}","maxReceiveCount":"1"}""" },
        });
        await using var host = await AmazonSqsTestHost.StartAsync(
            localStack,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o =>
            {
                o.VisibilityTimeout = TimeSpan.FromSeconds(1);
                o.Listen(queue);
            });

        await host.Transport.SendAsync(Message("poison-3", queue, 5, null), TestContext.Current.CancellationToken);

        var redriven = await localStack.ReceiveOneAsync(redriveQueue, Timeout);
        Assert.Equal("poison-3", AmazonSqsMapping.ToIncoming(redriven, redriveQueue).MessageId);
    }

    [Fact]
    public async Task TransientHandlerFailure_IsRetriedWithAGrowingAttemptCount()
    {
        var queue = Name("orders");
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(5);
        await using var host = await AmazonSqsTestHost.StartAsync(
            localStack,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(queue));

        await host.Transport.SendAsync(Message("flaky-5", queue, 5, null), TestContext.Current.CancellationToken);
        await gate.WaitForFailuresAsync(2, Timeout);

        Assert.Empty(journal.Handled);

        gate.Release(5);
        await journal.WaitForAsync(5, Timeout);
        await WaitForEmptyAsync(queue);

        Assert.Equal([5], journal.Handled);
    }

    [Fact]
    public async Task ManyHeadersAndBinaryBody_RoundTripThroughSqs()
    {
        var queue = Name("orders");
        await localStack.CreateQueueAsync(queue);
        await using var host = await AmazonSqsTestHost.StartAsync(localStack, new Journal(), new FailureGate(), _ => { });
        var headers = Enumerable.Range(1, 15).ToDictionary(i => $"x-header-{i:00}", i => $"value {i}");
        headers["bad name:with colon"] = "kept";
        byte[] body = [0, 1, 2, 250, 251, 252];

        await host.Transport.SendAsync(
            new TransportMessage("binary-1", "order-placed", queue, body, "application/octet-stream", headers, "customer-1"),
            TestContext.Current.CancellationToken);

        var received = await localStack.ReceiveOneAsync(queue, Timeout);
        var incoming = AmazonSqsMapping.ToIncoming(received, queue);
        Assert.True(received.MessageAttributes.Count <= AmazonSqsMapping.MaxAttributes);
        Assert.Equal(body, incoming.Body.ToArray());
        Assert.Equal("binary-1", incoming.MessageId);
        Assert.Equal("customer-1", incoming.PartitionKey);
        Assert.Equal(1, incoming.DeliveryAttempt);
        foreach (var (name, value) in headers)
        {
            Assert.Equal(value, incoming.Headers[name]);
        }
    }

    [Fact]
    public async Task MissingQueueOrTopic_IsPermanentFailure()
    {
        await using var host = await AmazonSqsTestHost.StartAsync(localStack, new Journal(), new FailureGate(), _ => { }, o => o.AutoCreate = false);
        var cancellation = TestContext.Current.CancellationToken;

        var queueError = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(Message("lost-1", Name("missing"), 1, null), cancellation));
        var topicError = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(Message("lost-2", AmazonSqsTransport.TopicPrefix + Name("missing"), 1, null), cancellation));

        Assert.Contains("SQS queue", queueError.Message, StringComparison.Ordinal);
        Assert.Contains("SNS topic", topicError.Message, StringComparison.Ordinal);
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

    private async Task WaitForEmptyAsync(string queue)
    {
        using var cts = new CancellationTokenSource(Timeout);
        while (await localStack.CountAsync(queue) > 0)
        {
            await Task.Delay(100, cts.Token);
        }
    }
}
