using System.Text;
using Twinbox.Storage;

namespace Twinbox.Testing.Conformance;

/// <summary>The behaviour every <see cref="IOutboxAdmin"/> must have. Each case gets a fresh, empty store that also
/// implements <see cref="IOutboxStore"/>, so the cases run next to <see cref="OutboxStoreConformance"/>.</summary>
public static class OutboxAdminConformance
{
    private const string Owner = "owner-a";
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset T1 = T0.AddHours(1);
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    public static IReadOnlyList<ConformanceCase<IOutboxStore>> Cases { get; } =
    [
        new("Admin query filters by status, destination, name and search", QueryFilters),
        new("Admin query pages newest first with a cursor", QueryPages),
        new("Admin get returns the message, or null when missing", GetReturnsMessage),
        new("Admin replay resets dead and sent messages only", ReplayResetsDeadAndSent),
        new("Admin delete removes only the given messages", DeleteRemovesGivenMessages),
    ];

    private static async Task QueryFilters(IOutboxStore store)
    {
        var admin = AdminOf(store);
        var dead = NewMessage(destination: "payments", name: "payment-failed");
        var sent = NewMessage(partitionKey: "order-7");
        var pending = NewMessage(partitionKey: "order-8", availableAt: T0.AddDays(1));
        await store.AppendAsync([dead, sent, pending], default);
        await store.ClaimAsync(new OutboxClaim(Owner, 100, T0, Lease), default);
        await store.CompleteAsync(
            Owner,
            [
                new DispatchOutcome(dead.Id, OutboxMessageStatus.Dead, 3, Error: "gave up"),
                new DispatchOutcome(sent.Id, OutboxMessageStatus.Sent, 1, SentAt: T0),
            ],
            default);

        await ExpectIds(admin, new OutboxQuery(), [pending.Id, sent.Id, dead.Id], "no filter");
        await ExpectIds(admin, new OutboxQuery { Status = OutboxMessageStatus.Dead }, [dead.Id], "status Dead");
        await ExpectIds(admin, new OutboxQuery { Status = OutboxMessageStatus.Sent }, [sent.Id], "status Sent");
        await ExpectIds(admin, new OutboxQuery { Status = OutboxMessageStatus.Pending }, [pending.Id], "status Pending");
        await ExpectIds(admin, new OutboxQuery { Destination = "payments" }, [dead.Id], "destination");
        await ExpectIds(admin, new OutboxQuery { MessageName = "order-placed" }, [pending.Id, sent.Id], "message name");
        await ExpectIds(admin, new OutboxQuery { Search = "order-7" }, [sent.Id], "search by partition key");
        await ExpectIds(admin, new OutboxQuery { Search = pending.Id.ToString() }, [pending.Id], "search by id");
        await ExpectIds(admin, new OutboxQuery { Search = "missing" }, [], "search without matches");
        await ExpectIds(admin, new OutboxQuery { Status = OutboxMessageStatus.Dead, Destination = "orders" }, [], "combined filters");

        var row = (await admin.QueryAsync(new OutboxQuery { Status = OutboxMessageStatus.Dead }, default)).Messages.Single();
        Expect(row.LastError == "gave up" && row.Attempts == 3, "query did not return the stored error and attempts");
    }

    private static async Task QueryPages(IOutboxStore store)
    {
        var admin = AdminOf(store);
        var messages = Enumerable.Range(0, 5).Select(_ => NewMessage()).ToArray();
        await store.AppendAsync(messages, default);
        var newestFirst = messages.Select(m => m.Id).Reverse().ToArray();

        var first = await admin.QueryAsync(new OutboxQuery { Take = 2 }, default);
        Expect(first.Messages.Select(m => m.Id).SequenceEqual(newestFirst[..2]), "first page is not the two newest messages");
        Expect(first.NextCursor is not null, "first page has no cursor although more messages exist");

        var second = await admin.QueryAsync(new OutboxQuery { Take = 2, Cursor = first.NextCursor }, default);
        Expect(second.Messages.Select(m => m.Id).SequenceEqual(newestFirst[2..4]), "second page does not continue after the first");
        Expect(second.NextCursor is not null, "second page has no cursor although more messages exist");

        var last = await admin.QueryAsync(new OutboxQuery { Take = 2, Cursor = second.NextCursor }, default);
        Expect(last.Messages.Select(m => m.Id).SequenceEqual(newestFirst[4..]), "last page does not hold the oldest message");
        Expect(last.NextCursor is null, "last page has a cursor although no messages are left");

        var exact = await admin.QueryAsync(new OutboxQuery { Take = 5 }, default);
        Expect(exact.Messages.Count == 5 && exact.NextCursor is null, "a page holding every message still has a cursor");
    }

    private static async Task GetReturnsMessage(IOutboxStore store)
    {
        var admin = AdminOf(store);
        var message = NewMessage(partitionKey: "p") with
        {
            Headers = new Dictionary<string, string> { ["x-correlation-id"] = "abc" },
        };
        await store.AppendAsync([message], default);

        var row = await admin.GetAsync(message.Id, default);

        Expect(row is not null, "get did not find an existing message");
        Expect(row!.Payload.AsSpan().SequenceEqual(message.Payload), "get did not return the payload");
        Expect(row.Headers.TryGetValue("x-correlation-id", out var value) && value == "abc", "get did not return the headers");
        Expect(row.PartitionKey == "p" && row.Status == OutboxMessageStatus.Pending, "get did not return the metadata");
        Expect(await admin.GetAsync(Guid.NewGuid(), default) is null, "get returned a message for an unknown id");
    }

    private static async Task ReplayResetsDeadAndSent(IOutboxStore store)
    {
        var admin = AdminOf(store);
        var dead = NewMessage();
        var sent = NewMessage();
        var processing = NewMessage();
        var pending = NewMessage(availableAt: T0.AddDays(1));
        await store.AppendAsync([dead, sent, processing, pending], default);
        await store.ClaimAsync(new OutboxClaim(Owner, 100, T0, Lease), default);
        await store.CompleteAsync(
            Owner,
            [
                new DispatchOutcome(dead.Id, OutboxMessageStatus.Dead, 5, Error: "gave up"),
                new DispatchOutcome(sent.Id, OutboxMessageStatus.Sent, 1, SentAt: T0),
            ],
            default);

        var changed = await admin.ReplayAsync([dead.Id, sent.Id, processing.Id, pending.Id, Guid.NewGuid()], T1, default);

        Expect(changed == 2, $"expected 2 replayed messages, got {changed}");
        foreach (var id in new[] { dead.Id, sent.Id })
        {
            var row = await admin.GetAsync(id, default);
            Expect(row?.Status == OutboxMessageStatus.Pending, $"a replayed message is {row?.Status}, not Pending");
            Expect(row!.Attempts == 0, $"a replayed message kept {row.Attempts} attempts");
            Expect(row.AvailableAt == T1, $"a replayed message is due at {row.AvailableAt}, not {T1}");
            Expect(row.LastError is null && row.SentAt is null, "a replayed message kept its error or sent time");
            Expect(row.LeaseOwner is null && row.LeaseUntil is null, "a replayed message kept a lease");
        }

        var leased = await admin.GetAsync(processing.Id, default);
        Expect(leased?.Status == OutboxMessageStatus.Processing && leased.LeaseOwner == Owner, "replay changed a leased message");
        var waiting = await admin.GetAsync(pending.Id, default);
        Expect(waiting?.Status == OutboxMessageStatus.Pending && waiting.AvailableAt == pending.AvailableAt, "replay changed a pending message");

        var claimed = await store.ClaimAsync(new OutboxClaim(Owner, 100, T1 + Lease, Lease), default);
        Expect(
            claimed.Select(m => m.Id).ToHashSet().SetEquals([dead.Id, sent.Id, processing.Id]),
            "replayed messages were not claimable again");
    }

    private static async Task DeleteRemovesGivenMessages(IOutboxStore store)
    {
        var admin = AdminOf(store);
        var doomed = NewMessage();
        var alsoDoomed = NewMessage();
        var kept = NewMessage();
        await store.AppendAsync([doomed, alsoDoomed, kept], default);

        var deleted = await admin.DeleteAsync([doomed.Id, alsoDoomed.Id, Guid.NewGuid()], default);

        Expect(deleted == 2, $"expected 2 deleted messages, got {deleted}");
        Expect(await admin.GetAsync(doomed.Id, default) is null, "a deleted message is still there");
        Expect(await admin.GetAsync(kept.Id, default) is not null, "delete removed a message it wasn't given");
        Expect(await admin.DeleteAsync([], default) == 0, "deleting no ids reported deletions");
    }

    private static async Task ExpectIds(IOutboxAdmin admin, OutboxQuery query, Guid[] expected, string filter)
    {
        var page = await admin.QueryAsync(query, default);
        Expect(
            page.Messages.Select(m => m.Id).SequenceEqual(expected),
            $"{filter}: expected [{string.Join(", ", expected)}] newest first, got [{string.Join(", ", page.Messages.Select(m => m.Id))}]");
    }

    private static IOutboxAdmin AdminOf(IOutboxStore store) =>
        store as IOutboxAdmin ?? throw new ConformanceException($"{store.GetType().Name} does not implement IOutboxAdmin");

    private static OutboxMessage NewMessage(
        string? partitionKey = null,
        DateTimeOffset? availableAt = null,
        string destination = "orders",
        string name = "order-placed") => new()
    {
        Id = Guid.NewGuid(),
        MessageName = name,
        Transport = "test",
        Destination = destination,
        PartitionKey = partitionKey,
        Payload = Encoding.UTF8.GetBytes("""{"orderId":42}"""),
        ContentType = "application/json",
        CreatedAt = T0.AddMinutes(-1),
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
