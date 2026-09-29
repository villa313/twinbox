using Twinbox.Storage;

namespace Twinbox.InMemory;

/// <summary>Scoped stand-in for a database transaction: commits what was sent through <see cref="IOutbox"/>.</summary>
public sealed class InMemoryUnitOfWork(IOutboxSession session, IOutboxStore store, IDispatchSignal signal)
{
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        var pending = session.TakePending();
        if (pending.Count == 0)
        {
            return;
        }

        await store.AppendAsync(pending, cancellationToken).ConfigureAwait(false);
        signal.Notify();
    }
}
