using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.InMemory;

public sealed class InMemoryInboxStore(IOutboxStore outbox, IDispatchSignal signal, TimeProvider time) : IInboxStore
{
    private readonly IOutboxStore _outbox = outbox;
    private readonly IDispatchSignal _signal = signal;
    private readonly TimeProvider _time = time;
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

    public Task<IInboxLease?> TryBeginAsync(InboxEntry entry, IServiceProvider scopedServices, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        var key = (entry.MessageId, entry.Consumer);
        lock (_gate)
        {
            if (_processed.ContainsKey(key) || !_inFlight.Add(key))
            {
                return Task.FromResult<IInboxLease?>(null);
            }
        }

        return Task.FromResult<IInboxLease?>(new Lease(this, key, scopedServices.GetRequiredService<IOutboxSession>()));
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

    private sealed class Lease(InMemoryInboxStore store, (string, string) key, IOutboxSession session) : IInboxLease
    {
        private bool _completed;

        public async Task CompleteAsync(CancellationToken cancellationToken)
        {
            var pending = session.TakePending();
            if (pending.Count > 0)
            {
                await store._outbox.AppendAsync(pending, cancellationToken).ConfigureAwait(false);
                store._signal.Notify();
            }

            lock (store._gate)
            {
                store._processed[key] = store._time.GetUtcNow();
                store._inFlight.Remove(key);
            }

            _completed = true;
        }

        public ValueTask DisposeAsync()
        {
            if (!_completed)
            {
                // Rolled back: the handler's outgoing messages are discarded along with the inbox entry.
                session.TakePending();
                lock (store._gate)
                {
                    store._inFlight.Remove(key);
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
