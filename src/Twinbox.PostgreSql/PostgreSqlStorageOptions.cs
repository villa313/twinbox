namespace Twinbox.PostgreSql;

public sealed class PostgreSqlStorageOptions
{
    public string? ConnectionString { get; set; }

    /// <summary>Resolved per scope, e.g. to pick a tenant's database; takes precedence over <see cref="ConnectionString"/>.</summary>
    public Func<IServiceProvider, string>? ConnectionStringFactory { get; set; }

    /// <summary>Null uses the database default (public).</summary>
    public string? Schema { get; set; }

    public string OutboxTable { get; set; } = "TwinboxOutbox";

    public string InboxTable { get; set; } = "TwinboxInbox";

    /// <summary>Creates the tables on startup; turn off when your migrations own the schema.</summary>
    public bool CreateSchemaIfMissing { get; set; } = true;
}
