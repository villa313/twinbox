using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Twinbox.MongoDB;

/// <summary>Creates the indexes once per database, so a tenant's database is prepared on first use.</summary>
internal sealed partial class MongoIndexInitializer(MongoSettings settings, ILogger<MongoIndexInitializer> logger)
{
    private readonly ILogger _logger = logger;

    private readonly ConcurrentDictionary<string, Lazy<Task>> _prepared = new(StringComparer.Ordinal);

    public async Task EnsureAsync(IMongoDatabase database, CancellationToken cancellationToken)
    {
        if (!settings.CreateIndexes)
        {
            return;
        }

        var key = $"{string.Join(',', database.Client.Settings.Servers)}/{database.DatabaseNamespace.DatabaseName}";
        var creation = _prepared.GetOrAdd(key, _ => new Lazy<Task>(() => CreateAsync(database, cancellationToken)));
        try
        {
            await creation.Value.ConfigureAwait(false);
        }
        catch
        {
            _prepared.TryRemove(key, out _);
            throw;
        }
    }

    private async Task CreateAsync(IMongoDatabase database, CancellationToken cancellationToken)
    {
        await settings.Outbox(database).Indexes.CreateManyAsync(
            [
                Index(new BsonDocument(OutboxDocument.Sequence, 1), unique: true),
                Index(new BsonDocument { { OutboxDocument.Status, 1 }, { OutboxDocument.AvailableAt, 1 } }),
                Index(new BsonDocument { { OutboxDocument.PartitionKey, 1 }, { OutboxDocument.Status, 1 }, { OutboxDocument.Sequence, 1 } }),
            ],
            cancellationToken).ConfigureAwait(false);
        await settings.Inbox(database).Indexes.CreateOneAsync(
            Index(new BsonDocument(MongoInboxStore.ProcessedAt, 1)),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        LogIndexesReady(database.DatabaseNamespace.DatabaseName);
    }

    private static CreateIndexModel<BsonDocument> Index(BsonDocument keys, bool unique = false) =>
        new(keys, new CreateIndexOptions { Unique = unique });

    [LoggerMessage(Level = LogLevel.Debug, Message = "Twinbox indexes are ready in MongoDB database {Database}")]
    private partial void LogIndexesReady(string database);
}
