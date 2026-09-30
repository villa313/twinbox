using System.Text;
using StackExchange.Redis;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.RedisStreams.Tests;

[Trait("Category", "Integration")]
public sealed class RedisStreamsIntegrationTests(RedisFixture redis) : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task OutboxMessage_IsConsumedByItsHandlerOnceAndAcknowledged()
    {
        var (stream, group) = Names();
        var journal = new Journal();
        await using var host = await RedisStreamsTestHost.StartAsync(
            redis.Configuration,
            journal,
            new FailureGate(),
            b => b.Route<OrderPlaced>().To(stream).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(stream, group));

        await host.SendAsync(new OrderPlaced(1));
        await journal.WaitForAsync(1, Timeout);
        await redis.WaitForNoPendingAsync(stream, group, Timeout);
        await Task.Delay(300, TestContext.Current.CancellationToken);

        var handled = Assert.Single(journal.Entries);
        Assert.Equal(1, handled.OrderId);
        Assert.Equal(1, handled.DeliveryAttempt);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Outbox.Snapshot()).Status);
    }

    [Fact]
    public async Task IdleConsumerWithNothingPending_IsRemovedFromTheGroup()
    {
        var (stream, group) = Names();
        await redis.Database.StreamCreateConsumerGroupAsync(stream, group, "0-0", createStream: true);
        await redis.Database.StreamReadGroupAsync(stream, group, "replaced-instance", ">");
        await Task.Delay(400, TestContext.Current.CancellationToken);

        await using var host = await RedisStreamsTestHost.StartAsync(
            redis.Configuration,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.ClaimIdleAfter = TimeSpan.FromMilliseconds(200);
                o.RemoveIdleConsumersAfter = TimeSpan.FromMilliseconds(300);
                o.Listen(stream, group);
            });

        using var cts = new CancellationTokenSource(Timeout);
        while ((await redis.Database.StreamConsumerInfoAsync(stream, group)).Any(c => c.Name == "replaced-instance"))
        {
            await Task.Delay(50, cts.Token);
        }
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInbox()
    {
        var (stream, group) = Names();
        var journal = new Journal();
        await using var host = await RedisStreamsTestHost.StartAsync(
            redis.Configuration,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(stream, group));
        var duplicate = Message("dup-1", stream, 1, "customer-1");
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(Message("after-dup", stream, 2, "customer-1"), cancellation);
        await journal.WaitForAsync(2, Timeout);
        await redis.WaitForNoPendingAsync(stream, group, Timeout);

        // One consumer reads the stream in order, so the sentinel arriving means both copies were processed.
        Assert.Equal([1, 2], journal.Handled);
        Assert.Equal("customer-1", journal.Entries[0].PartitionKey);
        Assert.Equal(3, await redis.Database.StreamLengthAsync(stream));
    }

    [Fact]
    public async Task EachConsumerGroup_GetsItsOwnCopy()
    {
        var (stream, _) = Names();
        var billing = new Journal();
        var shipping = new Journal();
        await using var billingHost = await RedisStreamsTestHost.StartAsync(
            redis.Configuration, billing, new FailureGate(), b => b.AddHandler<RecordingHandler, OrderPlaced>(), o => o.Listen(stream, "billing"));
        await using var shippingHost = await RedisStreamsTestHost.StartAsync(
            redis.Configuration, shipping, new FailureGate(), b => b.AddHandler<RecordingHandler, OrderPlaced>(), o => o.Listen(stream, "shipping"));

        await billingHost.Transport.SendAsync(Message("fan-1", stream, 7, null), TestContext.Current.CancellationToken);
        await billing.WaitForAsync(7, Timeout);
        await shipping.WaitForAsync(7, Timeout);
        await Task.Delay(300, TestContext.Current.CancellationToken);

        Assert.Equal([7], billing.Handled);
        Assert.Equal([7], shipping.Handled);
    }

    [Fact]
    public async Task CrashedConsumersPendingEntry_IsReclaimedAndProcessed()
    {
        var (stream, group) = Names();
        await redis.Database.StreamCreateConsumerGroupAsync(stream, group, "0-0", createStream: true);
        await redis.Database.StreamAddAsync(stream, RedisStreamsMapping.ToEntry(Message("orphan-1", stream, 4, null)));
        var taken = await redis.Database.StreamReadGroupAsync(stream, group, "crashed", ">");
        Assert.Single(taken);
        var journal = new Journal();

        await using var host = await RedisStreamsTestHost.StartAsync(
            redis.Configuration,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.ClaimIdleAfter = TimeSpan.FromMilliseconds(300);
                o.Listen(stream, group);
            });
        await journal.WaitForAsync(4, Timeout);
        await redis.WaitForNoPendingAsync(stream, group, Timeout);

        var handled = Assert.Single(journal.Entries);
        Assert.Equal(2, handled.DeliveryAttempt);
    }

    [Fact]
    public async Task TransientHandlerFailure_IsLeftPendingAndRetriedWithTheNextAttempt()
    {
        var (stream, group) = Names();
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(5);
        await using var host = await RedisStreamsTestHost.StartAsync(
            redis.Configuration,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.ClaimIdleAfter = TimeSpan.FromMilliseconds(200);
                o.Listen(stream, group);
            });

        await host.Transport.SendAsync(Message("flaky-5", stream, 5, null), TestContext.Current.CancellationToken);
        await gate.WaitForFailuresAsync(2, Timeout);

        Assert.Empty(journal.Handled);
        Assert.Equal(1, await redis.PendingCountAsync(stream, group));

        gate.Release(5);
        await journal.WaitForAsync(5, Timeout);
        await redis.WaitForNoPendingAsync(stream, group, Timeout);

        Assert.True(Assert.Single(journal.Entries).DeliveryAttempt >= 3);
        Assert.Equal(0, await redis.Database.StreamLengthAsync(RedisStreamsMapping.DeadStream(stream)));
    }

    [Fact]
    public async Task PermanentHandlerFailure_IsMovedToTheDeadStreamAndAcknowledged()
    {
        var (stream, group) = Names();
        await using var host = await RedisStreamsTestHost.StartAsync(
            redis.Configuration,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o => o.Listen(stream, group));

        await host.Transport.SendAsync(Message("poison-1", stream, 3, "customer-3"), TestContext.Current.CancellationToken);

        var dead = Assert.Single(await redis.WaitForEntriesAsync(RedisStreamsMapping.DeadStream(stream), 1, Timeout));
        var original = Assert.Single(await redis.Database.StreamRangeAsync(stream));
        Assert.Equal("poison-1", (string?)dead[RedisStreamsMapping.IdField]);
        Assert.Equal("order-placed", (string?)dead[RedisStreamsMapping.NameField]);
        Assert.Equal("""{"orderId":3}""", Encoding.UTF8.GetString((byte[])dead[RedisStreamsMapping.BodyField]!));
        Assert.Contains("can never be handled", (string?)dead[RedisStreamsMapping.ErrorField], StringComparison.Ordinal);
        Assert.Equal($"{stream}:{original.Id}", (string?)dead[RedisStreamsMapping.OriginField]);
        Assert.Equal(group, (string?)dead[RedisStreamsMapping.GroupField]);
        await redis.WaitForNoPendingAsync(stream, group, Timeout);
    }

    [Fact]
    public async Task EntryFailingEveryDelivery_IsMovedToTheDeadStreamAfterMaxDeliveryAttempts()
    {
        var (stream, group) = Names();
        var gate = new FailureGate();
        gate.Hold(8);
        await using var host = await RedisStreamsTestHost.StartAsync(
            redis.Configuration,
            new Journal(),
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.ClaimIdleAfter = TimeSpan.FromMilliseconds(100);
                o.MaxDeliveryAttempts = 3;
                o.Listen(stream, group);
            });

        await host.Transport.SendAsync(Message("stuck-8", stream, 8, null), TestContext.Current.CancellationToken);

        var dead = Assert.Single(await redis.WaitForEntriesAsync(RedisStreamsMapping.DeadStream(stream), 1, Timeout));
        Assert.Equal("InvalidOperationException: Order 8 is held.", (string?)dead[RedisStreamsMapping.ErrorField]);
        await redis.WaitForNoPendingAsync(stream, group, Timeout);
        Assert.Equal(3, gate.Failures);
    }

    [Fact]
    public async Task SendingToAKeyOfAnotherType_IsPermanentFailure()
    {
        var (stream, _) = Names();
        await redis.Database.StringSetAsync(stream, "not a stream");
        await using var host = await RedisStreamsTestHost.StartAsync(redis.Configuration, new Journal(), new FailureGate(), _ => { });

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(Message("wrong-1", stream, 1, null), TestContext.Current.CancellationToken));

        Assert.Contains(stream, error.Message, StringComparison.Ordinal);
        Assert.IsType<RedisServerException>(error.InnerException);
    }

    [Fact]
    public async Task MaxLength_TrimsTheStreamApproximately()
    {
        var (stream, _) = Names();
        await using var host = await RedisStreamsTestHost.StartAsync(
            redis.Configuration, new Journal(), new FailureGate(), _ => { }, o => o.MaxLength = 10);
        var cancellation = TestContext.Current.CancellationToken;

        for (var i = 0; i < 500; i++)
        {
            await host.Transport.SendAsync(Message($"bulk-{i}", stream, i, null), cancellation);
        }

        var length = await redis.Database.StreamLengthAsync(stream);
        Assert.InRange(length, 10, 499);
    }

    private static (string Stream, string Group) Names()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return ($"orders-{suffix}", $"billing-{suffix}");
    }

    private static TransportMessage Message(string id, string stream, int orderId, string? partitionKey) => new(
        id,
        "order-placed",
        stream,
        Encoding.UTF8.GetBytes($$"""{"orderId":{{orderId}}}"""),
        "application/json",
        new Dictionary<string, string>(),
        partitionKey);
}
