using System.Text;
using Azure.Messaging.EventHubs;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.EventHubs.Tests.Integration;

[Trait("Category", "Integration")]
public sealed class EventHubsIntegrationTests(EventHubsFixture emulator) : IClassFixture<EventHubsFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task OutboxMessage_IsConsumedByItsHandlerOnce()
    {
        var container = ContainerName();
        var journal = new Journal();
        await using var host = await EventHubsTestHost.StartAsync(
            emulator,
            journal,
            new FailureGate(),
            b => b.Route<OrderPlaced>().To(Hubs.Once).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(Hubs.Once, "$Default", emulator.StorageConnectionString, container));

        await host.SendAsync(new OrderPlaced(1));
        await journal.WaitForAsync(1, Timeout);
        await WaitForCheckpointAsync(container, Hubs.Once, "0", 0);

        Assert.Equal([1], journal.Handled);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Outbox.Snapshot()).Status);
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInbox()
    {
        var journal = new Journal();
        await using var host = await EventHubsTestHost.StartAsync(
            emulator,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(Hubs.Duplicates, "$Default", emulator.StorageConnectionString, ContainerName()));
        var duplicate = Message("dup-1", Hubs.Duplicates, 1, "customer-1");
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(Message("after-dup", Hubs.Duplicates, 2, "customer-1"), cancellation);
        await journal.WaitForAsync(2, Timeout);

        // One partition is read in order, so the sentinel arriving means both copies were processed.
        Assert.Equal([1, 2], journal.Handled);
    }

    [Fact]
    public async Task MessagesWithTheSamePartitionKey_AreHandledInOrder()
    {
        var journal = new Journal();
        await using var host = await EventHubsTestHost.StartAsync(
            emulator,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(Hubs.Ordered, "$Default", emulator.StorageConnectionString, ContainerName()));
        var cancellation = TestContext.Current.CancellationToken;

        for (var orderId = 1; orderId <= 30; orderId++)
        {
            await host.Transport.SendAsync(Message($"order-{orderId}", Hubs.Ordered, orderId, $"customer-{orderId % 3}"), cancellation);
        }

        await journal.WaitForCountAsync(30, Timeout);

        foreach (var perKey in journal.Entries.GroupBy(e => e.PartitionKey))
        {
            var orders = perKey.Select(e => e.OrderId).ToArray();
            Assert.Equal(orders.Order(), orders);
            Assert.Equal(10, orders.Length);
        }
    }

    [Fact]
    public async Task PermanentHandlerFailure_IsCopiedToTheDeadLetterHubAndCheckpointedPast()
    {
        var container = ContainerName();
        await using var host = await EventHubsTestHost.StartAsync(
            emulator,
            new Journal(),
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o =>
            {
                o.DeadLetterEventHub = Hubs.DeadLetters;
                o.Listen(Hubs.Poison, "$Default", emulator.StorageConnectionString, container);
            });

        await host.Transport.SendAsync(Message("poison-1", Hubs.Poison, 3, "customer-3"), TestContext.Current.CancellationToken);

        var deadLettered = await emulator.ReadUntilAsync(Hubs.DeadLetters, e => e.MessageId == "poison-1", Timeout);
        Assert.Equal("customer-3", deadLettered.PartitionKey);
        Assert.Equal("poison-1", deadLettered.Properties[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", deadLettered.Properties[TransportHeaders.MessageName]);
        Assert.Contains("can never be handled", (string)deadLettered.Properties[EventHubsMapping.ErrorProperty], StringComparison.Ordinal);
        Assert.Equal($"{Hubs.Poison}:0:0", deadLettered.Properties[EventHubsMapping.OriginProperty]);
        await WaitForCheckpointAsync(container, Hubs.Poison, "0", 0);
    }

    [Fact]
    public async Task TransientHandlerFailure_IsRetriedWithoutBlockingAnotherPartition()
    {
        var container = ContainerName();
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(1);
        await using var host = await EventHubsTestHost.StartAsync(
            emulator,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                // Far longer than the other partition needs, so it can only finish while partition 0 waits.
                o.RetryDelay = TimeSpan.FromSeconds(10);
                o.MaxRetryDelay = TimeSpan.FromSeconds(10);
                o.Listen(Hubs.Flaky, "$Default", emulator.StorageConnectionString, container);
            });

        await emulator.SendToPartitionAsync(Hubs.Flaky, "0", Events(Hubs.Flaky, 1, 2, 3));
        await gate.WaitForFailuresAsync(1, Timeout);
        await emulator.SendToPartitionAsync(Hubs.Flaky, "1", Events(Hubs.Flaky, 11, 12, 13, 14, 15));
        await journal.WaitForAsync(15, TimeSpan.FromSeconds(8));

        Assert.Equal([11, 12, 13, 14, 15], journal.Handled);
        Assert.Null(await emulator.CheckpointAsync(container, Hubs.Flaky, "0"));

        gate.Release(1);
        await journal.WaitForAsync(3, Timeout);

        Assert.Equal([1, 2, 3], journal.Handled.Where(id => id < 10));
        Assert.Equal(1, gate.Failures);
        Assert.Equal(2, journal.Entries.Single(e => e.OrderId == 1).Attempt);
        await WaitForCheckpointAsync(container, Hubs.Flaky, "0", 2);
    }

    [Fact]
    public async Task BatchHandler_ReceivesEventsOfAPartitionTogether()
    {
        var container = ContainerName();
        await emulator.SendToPartitionAsync(Hubs.Batched, "0", Events(Hubs.Batched, [.. Enumerable.Range(1, 30)]));
        var journal = new Journal();
        await using var host = await EventHubsTestHost.StartAsync(
            emulator,
            journal,
            new FailureGate(),
            b => b.AddBatchHandler<RecordingBatchHandler, OrderPlaced>(),
            o =>
            {
                o.MaxBatchSize = 10;
                o.Listen(Hubs.Batched, "$Default", emulator.StorageConnectionString, container);
            });

        await journal.WaitForAsync(30, Timeout);
        await WaitForCheckpointAsync(container, Hubs.Batched, "0", 29);

        Assert.Equal(Enumerable.Range(1, 30), journal.Handled);
        Assert.Contains(host.BatchLog.Sizes, size => size > 1);
        Assert.All(host.BatchLog.Sizes, size => Assert.InRange(size, 1, 10));
    }

    [Fact]
    public async Task BatchWithAPermanentFailure_DeadLettersOnlyTheBadEvent()
    {
        var container = ContainerName();
        await emulator.SendToPartitionAsync(Hubs.BatchedPoison, "0", Events(Hubs.BatchedPoison, 97, 98, 99, 100, 101));
        var journal = new Journal();
        await using var host = await EventHubsTestHost.StartAsync(
            emulator,
            journal,
            new FailureGate(),
            b => b.AddBatchHandler<RecordingBatchHandler, OrderPlaced>(),
            o =>
            {
                o.MaxBatchSize = 10;
                o.DeadLetterEventHub = Hubs.DeadLetters;
                o.Listen(Hubs.BatchedPoison, "$Default", emulator.StorageConnectionString, container);
            });

        var poison = $"order-{RecordingBatchHandler.Poison}";
        var deadLettered = await emulator.ReadUntilAsync(Hubs.DeadLetters, e => e.MessageId == poison, Timeout);
        await WaitForCheckpointAsync(container, Hubs.BatchedPoison, "0", 4);

        Assert.Equal($"{Hubs.BatchedPoison}:0:2", deadLettered.Properties[EventHubsMapping.OriginProperty]);
        Assert.Equal([97, 98, 100, 101], journal.Handled);
    }

    [Fact]
    public async Task OversizedMessage_IsPermanentFailure()
    {
        await using var host = await EventHubsTestHost.StartAsync(emulator, new Journal(), new FailureGate(), _ => { });
        var oversized = new TransportMessage(
            "huge-1", "order-placed", Hubs.SendOnly, new byte[2 * 1024 * 1024], "application/json", new Dictionary<string, string>(), null);

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(oversized, TestContext.Current.CancellationToken));

        Assert.Contains(Hubs.SendOnly, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnknownEventHub_IsPermanentFailure()
    {
        await using var host = await EventHubsTestHost.StartAsync(emulator, new Journal(), new FailureGate(), _ => { });

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(Message("lost-1", "no-such-hub", 1, null), TestContext.Current.CancellationToken));

        Assert.Equal(EventHubsException.FailureReason.ResourceNotFound, Assert.IsType<EventHubsException>(error.InnerException).Reason);
    }

    private static string ContainerName() => $"checkpoints-{Guid.NewGuid():N}";

    private static TransportMessage Message(string id, string eventHub, int orderId, string? partitionKey) => new(
        id,
        "order-placed",
        eventHub,
        Encoding.UTF8.GetBytes($$"""{"orderId":{{orderId}}}"""),
        "application/json",
        new Dictionary<string, string>(),
        partitionKey);

    private static List<EventData> Events(string eventHub, params int[] orderIds) =>
        [.. orderIds.Select(id => EventHubsMapping.ToEventData(Message($"order-{id}", eventHub, id, null)))];

    private async Task WaitForCheckpointAsync(string container, string eventHub, string partitionId, long sequenceNumber)
    {
        using var cts = new CancellationTokenSource(Timeout);
        while (await emulator.CheckpointAsync(container, eventHub, partitionId) != sequenceNumber)
        {
            await Task.Delay(100, cts.Token);
        }
    }
}
