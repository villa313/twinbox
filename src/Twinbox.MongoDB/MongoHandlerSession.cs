using MongoDB.Driver;

namespace Twinbox;

/// <summary>The session a handler must pass to its MongoDB writes so they commit with the inbox entry and its messages.</summary>
public sealed class MongoHandlerSession
{
    private IClientSessionHandle? _session;
    private IMongoDatabase? _database;

    public bool IsActive => _session is not null;

    public IClientSessionHandle Session => _session ?? throw NotActive();

    /// <summary>The Twinbox database, reached through the session's client; a session can't span clients.</summary>
    public IMongoDatabase Database => _database ?? throw NotActive();

    internal void Attach(IClientSessionHandle session, IMongoDatabase database)
    {
        _session = session;
        _database = database;
    }

    internal void Detach()
    {
        _session = null;
        _database = null;
    }

    private static InvalidOperationException NotActive() => new(
        "No handler session is active. MongoHandlerSession is only available inside a handler run by the MongoDB inbox.");
}
