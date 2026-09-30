namespace Twinbox.Storage;

/// <summary>Optional batch counterpart of <see cref="IInboxStore"/>; stores without it fall back to one message at a time.</summary>
public interface IBatchInboxStore
{
    /// <summary>
    /// Records the entries this consumer hasn't processed yet and runs <paramref name="handler"/> once with their indexes,
    /// all in one unit of work. Returns how many entries were new; the handler isn't called when there are none.
    /// </summary>
    Task<int> TryProcessBatchAsync(
        IReadOnlyList<InboxEntry> entries,
        IServiceProvider scopedServices,
        Func<IReadOnlyList<int>, CancellationToken, Task> handler,
        CancellationToken cancellationToken);
}
