using MongoDB.Bson;
using MongoDB.Driver;

namespace Twinbox.MongoDB;

internal sealed record MongoSettings(
    Func<IServiceProvider, IMongoClient> Client,
    Func<IServiceProvider, string> DatabaseName,
    string OutboxCollection,
    string InboxCollection,
    bool CreateIndexes)
{
    public IMongoCollection<BsonDocument> Outbox(IMongoDatabase database) =>
        database.GetCollection<BsonDocument>(OutboxCollection);

    public IMongoCollection<BsonDocument> Inbox(IMongoDatabase database) =>
        database.GetCollection<BsonDocument>(InboxCollection);

    /// <summary>Holds the counter that numbers outbox documents in insertion order.</summary>
    public IMongoCollection<BsonDocument> Sequences(IMongoDatabase database) =>
        database.GetCollection<BsonDocument>($"{OutboxCollection}_sequence");
}

/// <summary>Owns the client built from a connection string so it's created once and disposed with the container.</summary>
internal sealed class MongoClientOwner(string connectionString) : IDisposable
{
    private readonly Lazy<MongoClient> _client = new(() => new MongoClient(connectionString));

    public IMongoClient Client => _client.Value;

    public void Dispose()
    {
        if (_client.IsValueCreated)
        {
            _client.Value.Dispose();
        }
    }
}
