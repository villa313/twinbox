using System.Text;
using Twinbox.Storage;

namespace Twinbox.Testing.Conformance;

/// <summary>
/// The behaviour every <see cref="IOutboxStore"/> must have. Run each case against a fresh store, e.g. as an
/// xUnit theory over <see cref="Cases"/>.
/// </summary>
public static class OutboxStoreConformance
{
    private const string Owner = "owner-a";
    private const string OtherOwner = "owner-b";
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    public static IReadOnlyList<ConformanceCase<IOutboxStore>> Cases { get; } =
    [
        new("Claim leases due messages to the owner", ClaimLeasesDueMessages),
        new("Claim skips messages that are not yet available", ClaimSkipsFutureMessages),
        new("Claim skips messages leased to another owner until the lease expires", ClaimRespectsLeases),
        new("Claim returns only the head of each partition", ClaimReturnsPartitionHeads),
        new("Claim returns messages in insertion order", ClaimPreservesInsertionOrder),
        new("Claim respects the batch size", ClaimRespectsBatchSize),
        new("Complete marks sent messages and releases the lease", CompleteMarksSent),
        new("Complete reschedules retried messages", CompleteReschedules),
        new("Complete ignores messages leased to another owner", CompleteIgnoresOtherOwners),
        new("A dead message does not block its partition", DeadMessageUnblocksPartition),
        new("Purge removes expired sent messages only", PurgeRemovesExpiredSent),
        new("Statistics count pending and dead messages", StatisticsCountMessages),
        new("Payload, headers and metadata round-trip", RoundTripsFields),
    ];

    private static async Task ClaimLeasesDueMessages(IOutboxStore store)
    {
        var message = NewMessage();
        await store.AppendAsync([message], default);

        var claimed = await ClaimAsync(store, Owner, T0);

        Expect(claimed.Count == 1, $"expected 1 claimed message, got {claimed.Count}");
        var row = claimed[0];
        Expect(row.Id == message.Id, "claimed the wrong message");
        Expect(row.Status == OutboxMessageStatus.Processing, $"expected Processing, got {row.Status}");
        Expect(row.LeaseOwner == Owner, $"expected lease owner {Owner}, got {row.LeaseOwner}");
        Expect(row.LeaseUntil == T0 + Lease, $"expected lease until {T0 + Lease}, got {row.LeaseUntil}");
    }

    private static async Task ClaimSkipsFutureMessages(IOutboxStore store)
    {
        await store.AppendAsync([NewMessage(availableAt: T0.AddMinutes(1))], default);

        Expect((await ClaimAsync(store, Owner, T0)).Count == 0, "claimed a message before it was available");
        Expect((await ClaimAsync(store, Owner, T0.AddMinutes(1))).Count == 1, "did not claim a message once it became available");
    }

    private static async Task ClaimRespectsLeases(IOutboxStore store)
    {
        await store.AppendAsync([NewMessage()], default);
        await ClaimAsync(store, Owner, T0);

        Expect((await ClaimAsync(store, OtherOwner, T0.AddSeconds(10))).Count == 0, "claimed a message whose lease is still held");

        var reclaimed = await ClaimAsync(store, OtherOwner, T0 + Lease + TimeSpan.FromSeconds(1));
        Expect(reclaimed.Count == 1 && reclaimed[0].LeaseOwner == OtherOwner, "did not reclaim a message after its lease expired");
    }

    private static async Task ClaimReturnsPartitionHeads(IOutboxStore store)
    {
        var first = NewMessage(partitionKey: "order-1");
        var second = NewMessage(partitionKey: "order-1");
        var other = NewMessage(partitionKey: "order-2");
        var unkeyed = NewMessage();
        await store.AppendAsync([first, second, other, unkeyed], default);

        var claimed = await ClaimAsync(store, Owner, T0);
        var ids = claimed.Select(m => m.Id).ToHashSet();

        Expect(ids.SetEquals([first.Id, other.Id, unkeyed.Id]), "expected exactly the head of each partition plus unkeyed messages");

        await store.CompleteAsync(Owner, [Sent(first)], default);
        var next = await ClaimAsync(store, Owner, T0);
        Expect(next.Count == 1 && next[0].Id == second.Id, "the next message in the partition was not released after the head was sent");
    }

    private static async Task ClaimPreservesInsertionOrder(IOutboxStore store)
    {
        var messages = Enumerable.Range(0, 5).Select(_ => NewMessage()).ToArray();
        await store.AppendAsync(messages, default);

        var claimed = await ClaimAsync(store, Owner, T0);

        Expect(claimed.Select(m => m.Id).SequenceEqual(messages.Select(m => m.Id)), "messages were not claimed in insertion order");
    }

    private static async Task ClaimRespectsBatchSize(IOutboxStore store)
    {
        await store.AppendAsync([.. Enumerable.Range(0, 5).Select(_ => NewMessage())], default);

        var claimed = await store.ClaimAsync(new OutboxClaim(Owner, 2, T0, Lease), default);

        Expect(claimed.Count == 2, $"expected 2 claimed messages, got {claimed.Count}");
    }

    private static async Task CompleteMarksSent(IOutboxStore store)
    {
        var message = NewMessage();
        await store.AppendAsync([message], default);
        await ClaimAsync(store, Owner, T0);

        await store.CompleteAsync(Owner, [Sent(message)], default);

        Expect((await ClaimAsync(store, OtherOwner, T0.AddHours(1))).Count == 0, "a sent message was claimed again");
        var stats = await store.GetStatisticsAsync(default);
        Expect(stats.PendingCount == 0, $"expected no pending messages, got {stats.PendingCount}");
    }

    private static async Task CompleteReschedules(IOutboxStore store)
    {
        var message = NewMessage();
        await store.AppendAsync([message], default);
        await ClaimAsync(store, Owner, T0);

        var retryAt = T0.AddMinutes(5);
        await store.CompleteAsync(Owner, [new DispatchOutcome(message.Id, OutboxMessageStatus.Pending, 1, AvailableAt: retryAt, Error: "boom")], default);

        Expect((await ClaimAsync(store, Owner, T0.AddMinutes(4))).Count == 0, "a rescheduled message was claimed before its retry time");
        var retried = await ClaimAsync(store, Owner, retryAt);
        Expect(retried.Count == 1, "a rescheduled message was not claimed at its retry time");
        Expect(retried[0].Attempts == 1, $"expected 1 attempt, got {retried[0].Attempts}");
        Expect(retried[0].LastError == "boom", $"expected last error 'boom', got '{retried[0].LastError}'");
    }

    private static async Task CompleteIgnoresOtherOwners(IOutboxStore store)
    {
        var message = NewMessage();
        await store.AppendAsync([message], default);
        await ClaimAsync(store, Owner, T0);
        await ClaimAsync(store, OtherOwner, T0 + Lease + TimeSpan.FromSeconds(1));

        await store.CompleteAsync(Owner, [Sent(message)], default);

        var stats = await store.GetStatisticsAsync(default);
        Expect(stats.PendingCount == 1, "an owner that lost its lease was able to complete the message");
    }

    private static async Task DeadMessageUnblocksPartition(IOutboxStore store)
    {
        var first = NewMessage(partitionKey: "order-1");
        var second = NewMessage(partitionKey: "order-1");
        await store.AppendAsync([first, second], default);
        await ClaimAsync(store, Owner, T0);

        await store.CompleteAsync(Owner, [new DispatchOutcome(first.Id, OutboxMessageStatus.Dead, 10, Error: "gave up")], default);

        var next = await ClaimAsync(store, Owner, T0);
        Expect(next.Count == 1 && next[0].Id == second.Id, "a dead message kept blocking its partition");
    }

    private static async Task PurgeRemovesExpiredSent(IOutboxStore store)
    {
        var old = NewMessage();
        var recent = NewMessage();
        var pending = NewMessage(availableAt: T0.AddDays(1));
        await store.AppendAsync([old, recent, pending], default);
        await ClaimAsync(store, Owner, T0);
        await store.CompleteAsync(Owner, [Sent(old, T0), Sent(recent, T0.AddHours(2))], default);

        var purged = await store.PurgeAsync(new OutboxPurge(T0.AddHours(1), null, 100), default);

        Expect(purged == 1, $"expected 1 purged message, got {purged}");
        var stats = await store.GetStatisticsAsync(default);
        Expect(stats.PendingCount == 1, "purge removed a pending message");
    }

    private static async Task StatisticsCountMessages(IOutboxStore store)
    {
        var dead = NewMessage();
        var pending = NewMessage(createdAt: T0.AddMinutes(-3));
        await store.AppendAsync([dead, pending], default);
        await ClaimAsync(store, Owner, T0, batchSize: 1);
        await store.CompleteAsync(Owner, [new DispatchOutcome(dead.Id, OutboxMessageStatus.Dead, 1, Error: "x")], default);

        var stats = await store.GetStatisticsAsync(default);

        Expect(stats.PendingCount == 1, $"expected 1 pending, got {stats.PendingCount}");
        Expect(stats.DeadCount == 1, $"expected 1 dead, got {stats.DeadCount}");
        Expect(stats.OldestPendingCreatedAt == pending.CreatedAt, $"expected oldest pending {pending.CreatedAt}, got {stats.OldestPendingCreatedAt}");
    }

    private static async Task RoundTripsFields(IOutboxStore store)
    {
        var message = NewMessage(partitionKey: "p") with
        {
            TenantId = "tenant-1",
            Headers = new Dictionary<string, string> { ["x-correlation-id"] = "abc", ["x-empty"] = "" },
            TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        };
        await store.AppendAsync([message], default);

        var row = (await ClaimAsync(store, Owner, T0)).Single();

        Expect(row.MessageName == message.MessageName, "message name did not round-trip");
        Expect(row.Transport == message.Transport && row.Destination == message.Destination, "transport or destination did not round-trip");
        Expect(row.Payload.AsSpan().SequenceEqual(message.Payload), "payload did not round-trip");
        Expect(row.ContentType == message.ContentType, "content type did not round-trip");
        Expect(row.PartitionKey == "p" && row.TenantId == "tenant-1", "partition key or tenant did not round-trip");
        Expect(row.TraceParent == message.TraceParent, "trace parent did not round-trip");
        Expect(row.CreatedAt == message.CreatedAt, $"created-at did not round-trip ({row.CreatedAt} vs {message.CreatedAt})");
        Expect(row.Headers.Count == 2 && row.Headers["x-correlation-id"] == "abc" && row.Headers["x-empty"] == "", "headers did not round-trip");
    }

    private static Task<IReadOnlyList<OutboxMessage>> ClaimAsync(IOutboxStore store, string owner, DateTimeOffset now, int batchSize = 100) =>
        store.ClaimAsync(new OutboxClaim(owner, batchSize, now, Lease), default);

    private static DispatchOutcome Sent(OutboxMessage message, DateTimeOffset? at = null) =>
        new(message.Id, OutboxMessageStatus.Sent, 1, SentAt: at ?? T0);

    private static OutboxMessage NewMessage(string? partitionKey = null, DateTimeOffset? availableAt = null, DateTimeOffset? createdAt = null) => new()
    {
        Id = Guid.NewGuid(),
        MessageName = "order-placed",
        Transport = "test",
        Destination = "orders",
        PartitionKey = partitionKey,
        Payload = Encoding.UTF8.GetBytes("""{"orderId":42}"""),
        ContentType = "application/json",
        CreatedAt = createdAt ?? T0.AddMinutes(-1),
        AvailableAt = availableAt ?? T0,
    };

    private static void Expect(bool condition, string failure)
    {
        if (!condition)
        {
            throw new ConformanceException(failure);
        }
    }
}
