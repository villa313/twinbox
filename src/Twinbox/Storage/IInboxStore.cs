namespace Twinbox.Storage;

public interface IInboxStore
{
    /// <summary>
    /// Runs <paramref name="handler"/> in one unit of work with the inbox entry, so the entry, the handler's own writes
    /// and any messages it sends commit or roll back together. Returns false, without running the handler, when this
    /// consumer already processed the message. Stores with retrying execution strategies may run the handler again.
    /// </summary>
    Task<bool> TryProcessAsync(
        InboxEntry entry,
        IServiceProvider scopedServices,
        Func<CancellationToken, Task> handler,
        CancellationToken cancellationToken);

    Task<int> PurgeAsync(DateTimeOffset processedBefore, int batchSize, CancellationToken cancellationToken);
}

public sealed record InboxEntry(string MessageId, string Consumer, string Source, DateTimeOffset ReceivedAt);
