using MongoDB.Driver;

namespace Twinbox.MongoDB;

public sealed class MongoDBStorageOptions
{
    public string? ConnectionString { get; set; }

    /// <summary>Resolved per scope, e.g. to pick a tenant's cluster; takes precedence over <see cref="ConnectionString"/>. Return cached clients.</summary>
    public Func<IServiceProvider, IMongoClient>? ClientFactory { get; set; }

    public string? DatabaseName { get; set; }

    /// <summary>Resolved per scope, e.g. to pick a tenant's database; takes precedence over <see cref="DatabaseName"/>.</summary>
    public Func<IServiceProvider, string>? DatabaseNameFactory { get; set; }

    public string OutboxCollection { get; set; } = "twinbox_outbox";

    public string InboxCollection { get; set; } = "twinbox_inbox";

    /// <summary>Creates the indexes on startup; turn off when your deployment manages them.</summary>
    public bool CreateIndexes { get; set; } = true;
}
