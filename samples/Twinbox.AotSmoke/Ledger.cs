using System.Collections.Concurrent;

namespace Twinbox.AotSmoke;

public sealed class Ledger
{
    private readonly ConcurrentDictionary<(string Kind, int Id), int> _handled = new();
    private readonly ConcurrentDictionary<string, ConcurrentBag<string>> _messageIds = new();
    private int _filterCalls;

    public int FilterCalls => Volatile.Read(ref _filterCalls);

    public void Record(string kind, int id, string messageId)
    {
        _handled.AddOrUpdate((kind, id), 1, (_, count) => count + 1);
        _messageIds.GetOrAdd(kind, _ => []).Add(messageId);
    }

    public void RecordFilter() => Interlocked.Increment(ref _filterCalls);

    public IReadOnlyList<string> HandledMessageIds(string kind) =>
        _messageIds.TryGetValue(kind, out var ids) ? [.. ids] : [];

    public async Task<bool> WaitForAsync(IReadOnlyDictionary<string, int> expected, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (expected.All(e => _handled.Keys.Count(k => k.Kind == e.Key) >= e.Value))
            {
                return true;
            }

            await Task.Delay(50).ConfigureAwait(false);
        }

        return false;
    }

    public IEnumerable<string> Verify(IReadOnlyDictionary<string, int> expected)
    {
        foreach (var (kind, count) in expected)
        {
            var seen = _handled.Where(h => h.Key.Kind == kind).ToList();
            if (seen.Count != count)
            {
                yield return $"{kind}: {seen.Count} distinct messages handled, expected {count}";
            }

            foreach (var duplicate in seen.Where(h => h.Value != 1))
            {
                yield return $"{kind} {duplicate.Key.Id} handled {duplicate.Value} times";
            }
        }
    }

    public string Describe() =>
        string.Join(", ", _handled.GroupBy(h => h.Key.Kind).Select(g => $"{g.Key}={g.Count()}"));
}
