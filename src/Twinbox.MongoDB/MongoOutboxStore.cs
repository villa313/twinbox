using System.Globalization;
using MongoDB.Bson;
using MongoDB.Driver;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.MongoDB;

internal sealed class MongoOutboxStore(MongoSettings settings, MongoIndexInitializer indexes, TwinboxScopeFactory scopes)
    : IOutboxStore, IOutboxAdmin
{
    private const int Pending = (int)OutboxMessageStatus.Pending;
    private const int Processing = (int)OutboxMessageStatus.Processing;
    private const int Sent = (int)OutboxMessageStatus.Sent;
    private const int Dead = (int)OutboxMessageStatus.Dead;
    private const int MinCandidatePage = 100;

    private static readonly BsonDocument BySequence = new(OutboxDocument.Sequence, 1);
    private static readonly BsonDocument NewestFirst = new(OutboxDocument.Sequence, -1);
    private static readonly BsonDocument Unsent = new(OutboxDocument.Status, new BsonDocument("$in", new BsonArray { Pending, Processing }));

    public async Task AppendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await InsertAsync(lease.Database, session: null, messages, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Without SKIP LOCKED, a filtered findOneAndUpdate per document is the atomic step competing instances race on.</summary>
    public async Task<IReadOnlyList<OutboxMessage>> ClaimAsync(OutboxClaim claim, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(claim);
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        var outbox = settings.Outbox(lease.Database);
        var due = Due(claim.Now);
        var leaseUpdate = new BsonDocument("$set", new BsonDocument
        {
            { OutboxDocument.Status, Processing },
            { OutboxDocument.LeaseOwner, claim.Owner },
            { OutboxDocument.LeaseUntil, OutboxDocument.Timestamp(claim.Now + claim.LeaseDuration) },
        });
        var claimOptions = new FindOneAndUpdateOptions<BsonDocument> { ReturnDocument = ReturnDocument.After };
        var pageSize = Math.Max(claim.BatchSize, MinCandidatePage);
        var heads = new Dictionary<string, long>(StringComparer.Ordinal);
        var claimed = new List<OutboxMessage>();
        var after = long.MinValue;

        while (claimed.Count < claim.BatchSize)
        {
            var candidates = await outbox
                .Find(new BsonDocument("$and", new BsonArray { due, new BsonDocument(OutboxDocument.Sequence, new BsonDocument("$gt", after)) }))
                .Sort(BySequence)
                .Limit(pageSize)
                .Project(new BsonDocument { { OutboxDocument.Sequence, 1 }, { OutboxDocument.PartitionKey, 1 } })
                .ToListAsync(cancellationToken).ConfigureAwait(false);
            if (candidates.Count == 0)
            {
                break;
            }

            after = candidates[^1][OutboxDocument.Sequence].ToInt64();
            await LoadPartitionHeadsAsync(outbox, candidates, heads, cancellationToken).ConfigureAwait(false);

            foreach (var candidate in candidates)
            {
                if (claimed.Count == claim.BatchSize)
                {
                    break;
                }

                if (PartitionOf(candidate) is { } partition && heads[partition] != candidate[OutboxDocument.Sequence].ToInt64())
                {
                    continue;
                }

                var document = await outbox.FindOneAndUpdateAsync(
                    new BsonDocument("$and", new BsonArray { new BsonDocument(OutboxDocument.Id, candidate[OutboxDocument.Id]), due }),
                    leaseUpdate,
                    claimOptions,
                    cancellationToken).ConfigureAwait(false);

                // Null means another instance leased it between the scan and the update.
                if (document is not null)
                {
                    claimed.Add(OutboxDocument.ToMessage(document));
                }
            }

            if (candidates.Count < pageSize)
            {
                break;
            }
        }

        return claimed;
    }

    public async Task CompleteAsync(string owner, IReadOnlyList<DispatchOutcome> outcomes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(outcomes);
        if (outcomes.Count == 0)
        {
            return;
        }

        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        var updates = outcomes.Select(o => new UpdateOneModel<BsonDocument>(
            new BsonDocument
            {
                { OutboxDocument.Id, OutboxDocument.Key(o.MessageId) },
                { OutboxDocument.LeaseOwner, owner },
                { OutboxDocument.Status, Processing },
            },
            new BsonDocument("$set", Outcome(o))));
        await settings.Outbox(lease.Database)
            .BulkWriteAsync(updates, new BulkWriteOptions { IsOrdered = false }, cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<int> PurgeAsync(OutboxPurge purge, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(purge);
        var expired = new BsonArray
        {
            new BsonDocument { { OutboxDocument.Status, Sent }, { OutboxDocument.SentAt, new BsonDocument("$lt", OutboxDocument.Timestamp(purge.SentBefore)) } },
        };
        if (purge.DeadBefore is { } deadBefore)
        {
            expired.Add(new BsonDocument { { OutboxDocument.Status, Dead }, { OutboxDocument.CreatedAt, new BsonDocument("$lt", OutboxDocument.Timestamp(deadBefore)) } });
        }

        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        return await DeleteBatchAsync(settings.Outbox(lease.Database), new BsonDocument("$or", expired), purge.BatchSize, cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        var outbox = settings.Outbox(lease.Database);
        var pending = await outbox.CountDocumentsAsync(Unsent, cancellationToken: cancellationToken).ConfigureAwait(false);
        var oldest = await outbox.Find(Unsent)
            .Sort(new BsonDocument(OutboxDocument.CreatedAt, 1))
            .Limit(1)
            .Project(new BsonDocument(OutboxDocument.CreatedAt, 1))
            .FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var dead = await outbox.CountDocumentsAsync(new BsonDocument(OutboxDocument.Status, Dead), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return new OutboxStatistics(pending, oldest is null ? null : OutboxDocument.ReadTimestamp(oldest[OutboxDocument.CreatedAt]), dead);
    }

    public async Task<OutboxPage> QueryAsync(OutboxQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(query.Take);
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        var matches = await settings.Outbox(lease.Database)
            .Find(Filter(query))
            .Sort(NewestFirst)
            .Limit(query.Take + 1)
            .ToListAsync(cancellationToken).ConfigureAwait(false);

        var page = matches.Take(query.Take).ToArray();
        var next = matches.Count > query.Take
            ? page[^1][OutboxDocument.Sequence].ToInt64().ToString(CultureInfo.InvariantCulture)
            : null;
        return new OutboxPage([.. page.Select(OutboxDocument.ToMessage)], next);
    }

    public async Task<OutboxMessage?> GetAsync(Guid id, CancellationToken cancellationToken)
    {
        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        var document = await settings.Outbox(lease.Database)
            .Find(new BsonDocument(OutboxDocument.Id, OutboxDocument.Key(id)))
            .FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        return document is null ? null : OutboxDocument.ToMessage(document);
    }

    public async Task<int> ReplayAsync(IReadOnlyCollection<Guid> ids, DateTimeOffset now, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(ids);
        if (ids.Count == 0)
        {
            return 0;
        }

        var filter = new BsonDocument
        {
            { OutboxDocument.Id, IdIn(ids) },
            { OutboxDocument.Status, new BsonDocument("$in", new BsonArray { Sent, Dead }) },
        };
        var update = new BsonDocument("$set", new BsonDocument
        {
            { OutboxDocument.Status, Pending },
            { OutboxDocument.Attempts, 0 },
            { OutboxDocument.AvailableAt, OutboxDocument.Timestamp(now) },
            { OutboxDocument.LastError, BsonNull.Value },
            { OutboxDocument.SentAt, BsonNull.Value },
            { OutboxDocument.LeaseOwner, BsonNull.Value },
            { OutboxDocument.LeaseUntil, BsonNull.Value },
        });

        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        var result = await settings.Outbox(lease.Database).UpdateManyAsync(filter, update, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return (int)result.ModifiedCount;
    }

    public async Task<int> DeleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(ids);
        if (ids.Count == 0)
        {
            return 0;
        }

        await using var lease = await OpenAsync(cancellationToken).ConfigureAwait(false);
        var result = await settings.Outbox(lease.Database)
            .DeleteManyAsync(new BsonDocument(OutboxDocument.Id, IdIn(ids)), cancellationToken)
            .ConfigureAwait(false);
        return (int)result.DeletedCount;
    }

    /// <summary>Inserts through the caller's session and transaction; <paramref name="scopedServices"/> picks the database.</summary>
    internal async Task SaveAsync(
        IClientSessionHandle session,
        IServiceProvider scopedServices,
        IReadOnlyList<OutboxMessage> messages,
        CancellationToken cancellationToken)
    {
        var database = session.Client.GetDatabase(settings.DatabaseName(scopedServices));
        await indexes.EnsureAsync(database, cancellationToken).ConfigureAwait(false);
        await InsertAsync(database, session, messages, cancellationToken).ConfigureAwait(false);
    }

    internal async Task InsertAsync(
        IMongoDatabase database,
        IClientSessionHandle? session,
        IReadOnlyList<OutboxMessage> messages,
        CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }

        var first = await AllocateSequenceAsync(database, messages.Count, cancellationToken).ConfigureAwait(false);
        var documents = messages.Select((m, i) => OutboxDocument.From(m, first + i)).ToList();
        var outbox = settings.Outbox(database);
        var options = new InsertManyOptions { IsOrdered = true };
        if (session is null)
        {
            await outbox.InsertManyAsync(documents, options, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await outbox.InsertManyAsync(session, documents, options, cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task<DatabaseLease> OpenAsync(CancellationToken cancellationToken)
    {
        var scope = scopes.CreateAsyncScope();
        try
        {
            var database = settings.Client(scope.ServiceProvider).GetDatabase(settings.DatabaseName(scope.ServiceProvider));
            await indexes.EnsureAsync(database, cancellationToken).ConfigureAwait(false);
            return new DatabaseLease(scope, database);
        }
        catch
        {
            await scope.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Deletes up to <paramref name="batchSize"/> matches; deleteMany has no limit, so the ids are picked first.</summary>
    internal static async Task<int> DeleteBatchAsync(
        IMongoCollection<BsonDocument> collection,
        BsonDocument filter,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var ids = await collection.Find(filter)
            .Limit(batchSize)
            .Project(new BsonDocument(OutboxDocument.Id, 1))
            .ToListAsync(cancellationToken).ConfigureAwait(false);
        if (ids.Count == 0)
        {
            return 0;
        }

        var selected = new BsonDocument(OutboxDocument.Id, new BsonDocument("$in", new BsonArray(ids.Select(d => d[OutboxDocument.Id]))));
        var result = await collection.DeleteManyAsync(new BsonDocument("$and", new BsonArray { selected, filter }), cancellationToken)
            .ConfigureAwait(false);
        return (int)result.DeletedCount;
    }

    private static BsonDocument Filter(OutboxQuery query)
    {
        var filter = new BsonDocument();
        if (long.TryParse(query.Cursor, NumberStyles.None, CultureInfo.InvariantCulture, out var before))
        {
            filter.Add(OutboxDocument.Sequence, new BsonDocument("$lt", before));
        }

        if (query.Status is { } status)
        {
            filter.Add(OutboxDocument.Status, (int)status);
        }

        if (query.Destination is { } destination)
        {
            filter.Add("Destination", destination);
        }

        if (query.MessageName is { } name)
        {
            filter.Add("MessageName", name);
        }

        if (query.Search is { } search)
        {
            var byKey = new BsonDocument(OutboxDocument.PartitionKey, search);
            filter.AddRange(Guid.TryParse(search, out var id)
                ? new BsonDocument("$or", new BsonArray { new BsonDocument(OutboxDocument.Id, OutboxDocument.Key(id)), byKey })
                : byKey);
        }

        return filter;
    }

    private static BsonDocument IdIn(IReadOnlyCollection<Guid> ids) =>
        new("$in", new BsonArray(ids.Distinct().Select(OutboxDocument.Key)));

    private static BsonDocument Due(DateTimeOffset now)
    {
        var at = OutboxDocument.Timestamp(now);
        return new BsonDocument("$or", new BsonArray
        {
            new BsonDocument { { OutboxDocument.Status, Pending }, { OutboxDocument.AvailableAt, new BsonDocument("$lte", at) } },
            new BsonDocument { { OutboxDocument.Status, Processing }, { OutboxDocument.LeaseUntil, new BsonDocument("$lt", at) } },
        });
    }

    private static BsonDocument Outcome(DispatchOutcome o)
    {
        var set = new BsonDocument
        {
            { OutboxDocument.Status, (int)o.Status },
            { OutboxDocument.Attempts, o.Attempts },
            { OutboxDocument.SentAt, OutboxDocument.Timestamp(o.SentAt) },
            { OutboxDocument.LeaseOwner, BsonNull.Value },
            { OutboxDocument.LeaseUntil, BsonNull.Value },
        };
        if (o.AvailableAt is { } availableAt)
        {
            set.Add(OutboxDocument.AvailableAt, OutboxDocument.Timestamp(availableAt));
        }

        if (o.Error is { } error)
        {
            set.Add(OutboxDocument.LastError, error);
        }

        return set;
    }

    private static string? PartitionOf(BsonDocument document) =>
        document.GetValue(OutboxDocument.PartitionKey, BsonNull.Value) is { IsBsonNull: false } key ? key.AsString : null;

    /// <summary>Records the lowest unsent sequence of each newly seen partition; only that document may be claimed.</summary>
    private static async Task LoadPartitionHeadsAsync(
        IMongoCollection<BsonDocument> outbox,
        List<BsonDocument> candidates,
        Dictionary<string, long> heads,
        CancellationToken cancellationToken)
    {
        var unknown = candidates.Select(PartitionOf).OfType<string>().Where(k => !heads.ContainsKey(k)).Distinct(StringComparer.Ordinal).ToArray();
        if (unknown.Length == 0)
        {
            return;
        }

        BsonDocument[] pipeline =
        [
            new("$match", new BsonDocument(Unsent) { { OutboxDocument.PartitionKey, new BsonDocument("$in", new BsonArray(unknown)) } }),
            new("$group", new BsonDocument { { "_id", $"${OutboxDocument.PartitionKey}" }, { "Head", new BsonDocument("$min", $"${OutboxDocument.Sequence}") } }),
        ];
        using var cursor = await outbox.AggregateAsync<BsonDocument>(pipeline, cancellationToken: cancellationToken).ConfigureAwait(false);
        foreach (var group in await cursor.ToListAsync(cancellationToken).ConfigureAwait(false))
        {
            heads[group["_id"].AsString] = group["Head"].ToInt64();
        }
    }

    // $inc on one document is atomic, so concurrent writers get strictly increasing, disjoint blocks. It runs outside the
    // caller's transaction, like an identity column: inside, concurrent transactions would write-conflict on the counter.
    private async Task<long> AllocateSequenceAsync(IMongoDatabase database, int count, CancellationToken cancellationToken)
    {
        var counter = await settings.Sequences(database).FindOneAndUpdateAsync(
            new BsonDocument("_id", settings.OutboxCollection),
            new BsonDocument("$inc", new BsonDocument("Value", (long)count)),
            new FindOneAndUpdateOptions<BsonDocument> { IsUpsert = true, ReturnDocument = ReturnDocument.After },
            cancellationToken).ConfigureAwait(false);
        return counter["Value"].ToInt64() - count + 1;
    }

    internal sealed class DatabaseLease(Microsoft.Extensions.DependencyInjection.AsyncServiceScope scope, IMongoDatabase database) : IAsyncDisposable
    {
        public IMongoDatabase Database => database;

        public ValueTask DisposeAsync() => scope.DisposeAsync();
    }
}
