using System.Data.Common;

namespace Twinbox.Migration;

/// <summary>Drains unsent messages from another outbox table into Twinbox's outbox.</summary>
public sealed class OutboxImportOptions
{
    public Func<IServiceProvider, DbConnection>? CreateConnection { get; set; }

    /// <summary>Returns up to <c>@batch</c> rows with <c>Id</c>, <c>Name</c>, <c>Content</c> and optionally <c>Headers</c> (a JSON object) and <c>PartitionKey</c>.</summary>
    public string? SelectPending { get; set; }

    /// <summary>Marks one imported row so it isn't selected again; receives <c>@id</c> with the type <c>Id</c> was read as.</summary>
    public string? MarkImported { get; set; }

    /// <summary>The <see cref="Storage.IOutboxStore.Name"/> to import into; required only when several stores are registered.</summary>
    public string? Store { get; set; }

    public int BatchSize { get; set; } = 100;

    /// <summary>The old system may still be writing during the switch, so the table keeps being polled.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(10);
}

/// <summary>Records messages another system already processed, so Twinbox's inbox won't process them again.</summary>
public sealed class InboxSeedOptions
{
    public Func<IServiceProvider, DbConnection>? CreateConnection { get; set; }

    /// <summary>Returns columns <c>MessageId</c> and <c>Consumer</c>, where Consumer is the Twinbox handler's consumer name.</summary>
    public string? SelectProcessed { get; set; }
}
