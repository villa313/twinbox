using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.InMemory;

public sealed class InMemoryInboxStore(IOutboxStore outbox, IDispatchSignal signal, TimeProvider time) : IInboxStore, IBatchInboxStore
{
    private readonly object _gate = new();
    private readonly Dictionary<(string MessageId, string Consumer), DateTimeOffset> _processed = [];
    private readonly HashSet<(string MessageId, string Consumer)> _inFlight = [];

    public bool HasProcessed(string messageId, string consumer)
    {
        lock (_gate)
        {
            return _processed.ContainsKey((messageId, consumer));
        }
    }

    public async Task<bool> TryProcessAsync(
        InboxEntry entry,
        IServiceProvider scopedServices,
        Func<CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(scopedServices);
        ArgumentNullException.ThrowIfNull(handler);

        var key = (entry.MessageId, entry.Consumer);
        lock (_gate)
        {
            if (_processed.ContainsKey(key) || !_inFlight.Add(key))
            {
                return false;
            }
        }

        var session = scopedServices.GetRequiredService<IOutboxSession>();
        try
        {
            await handler(cancellationToken).ConfigureAwait(false);
            var pending = session.TakePending();
            if (pending.Count > 0)
            {
                await outbox.AppendAsync(pending, cancellationToken).ConfigureAwait(false);
                signal.Notify();
            }

            lock (_gate)
            {
                _processed[key] = time.GetUtcNow();
            }

            return true;
        }
        catch
        {
            // Rolled back: whatever the handler sent is discarded with the inbox entry.
            session.TakePending();
            throw;
        }
        finally
        {
            lock (_gate)
            {
                _inFlight.Remove(key);
            }
        }
    }

    public async Task<int> TryProcessBatchAsync(
        IReadOnlyList<InboxEntry> entries,
        IServiceProvider scopedServices,
        Func<IReadOnlyList<int>, CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(scopedServices);
        ArgumentNullException.ThrowIfNull(handler);

        var fresh = new List<int>();
        lock (_gate)
        {
            for (var i = 0; i < entries.Count; i++)
            {
                var key = (entries[i].MessageId, entries[i].Consumer);
                if (!_processed.ContainsKey(key) && _inFlight.Add(key))
                {
                    fresh.Add(i);
                }
            }
        }

        if (fresh.Count == 0)
        {
            return 0;
        }

        var session = scopedServices.GetRequiredService<IOutboxSession>();
        try
        {
            await handler(fresh, cancellationToken).ConfigureAwait(false);
            var pending = session.TakePending();
            if (pending.Count > 0)
            {
                await outbox.AppendAsync(pending, cancellationToken).ConfigureAwait(false);
                signal.Notify();
            }

            lock (_gate)
            {
                foreach (var i in fresh)
                {
                    _processed[(entries[i].MessageId, entries[i].Consumer)] = time.GetUtcNow();
                }
            }

            return fresh.Count;
        }
        catch
        {
            session.TakePending();
            throw;
        }
        finally
        {
            lock (_gate)
            {
                foreach (var i in fresh)
                {
                    _inFlight.Remove((entries[i].MessageId, entries[i].Consumer));
                }
            }
        }
    }

    public Task<int> PurgeAsync(DateTimeOffset processedBefore, int batchSize, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var expired = _processed.Where(p => p.Value < processedBefore).Take(batchSize).Select(p => p.Key).ToArray();
            foreach (var key in expired)
            {
                _processed.Remove(key);
            }

            return Task.FromResult(expired.Length);
        }
    }
}
