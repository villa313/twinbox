using Twinbox.Storage;

namespace Twinbox.Sql;

internal static class InboxLockOrder
{
    /// <summary>Entry indexes in one fixed order, so overlapping batches claim inbox keys in the same sequence and can't deadlock.</summary>
    public static IEnumerable<int> Of(IReadOnlyList<InboxEntry> entries) =>
        Enumerable.Range(0, entries.Count)
            .OrderBy(i => entries[i].MessageId, StringComparer.Ordinal)
            .ThenBy(i => entries[i].Consumer, StringComparer.Ordinal);
}
