using MongoDB.Bson;
using MongoDB.Driver;
using Testcontainers.MongoDb;

// One container for the whole run; every test isolates itself in its own database.
[assembly: AssemblyFixture(typeof(Twinbox.MongoDB.IntegrationTests.MongoFixture))]

namespace Twinbox.MongoDB.IntegrationTests;

/// <summary>A single-node replica set, since MongoDB only runs multi-document transactions on replica sets.</summary>
public sealed class MongoFixture : IAsyncLifetime
{
    private readonly MongoDbContainer _container = new MongoDbBuilder("mongo:8.0").WithReplicaSet().Build();
    private MongoClient? _client;

    public string ConnectionString => _container.GetConnectionString();

    /// <summary>The app's own client, separate from the one Twinbox builds from the connection string.</summary>
    public IMongoClient Client => _client ?? throw new InvalidOperationException("The fixture has not started.");

    public IMongoCollection<BsonDocument> Collection(string database, string name) =>
        Client.GetDatabase(database).GetCollection<BsonDocument>(name);

    public async Task<long> CountAsync(string database, string collection) =>
        await Collection(database, collection).CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _client = new MongoClient(ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        _client?.Dispose();
        await _container.DisposeAsync();
    }
}
