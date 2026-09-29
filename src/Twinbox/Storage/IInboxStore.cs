namespace Twinbox.Storage;

public interface IInboxStore
{
    /// <summary>
    /// Records <paramref name="entry"/> inside a unit of work the handler shares. Returns null when the
    /// consumer already processed this message.
    /// </summary>
    Task<IInboxLease?> TryBeginAsync(InboxEntry entry, IServiceProvider scopedServices, CancellationToken cancellationToken);

    Task<int> PurgeAsync(DateTimeOffset processedBefore, int batchSize, CancellationToken cancellationToken);
}

public sealed record InboxEntry(string MessageId, string Consumer, string Source, DateTimeOffset ReceivedAt);

/// <summary>Disposing without completing rolls back the entry and everything the handler wrote.</summary>
public interface IInboxLease : IAsyncDisposable
{
    Task CompleteAsync(CancellationToken cancellationToken);
}
