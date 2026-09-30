using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using Twinbox.MongoDB;
using Twinbox.Storage;

namespace Twinbox;

public static class MongoDBTransactionalOutboxExtensions
{
    /// <summary>Inserts the messages sent so far through <paramref name="session"/>; they're dispatched once its transaction commits.</summary>
    public static async Task SaveAsync(this IOutbox outbox, IClientSessionHandle session, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        var buffer = Buffer(outbox);
        var pending = buffer.TakePending();
        if (pending.Count == 0)
        {
            return;
        }

        var store = buffer.Services.GetService<MongoOutboxStore>()
            ?? throw new InvalidOperationException("Saving outbox messages through a MongoDB session needs the MongoDB store, and it isn't registered. "
                + "Call UseMongoDB(...) from Twinbox.MongoDB inside AddTwinbox.");
        await store.SaveAsync(session, buffer.Services, pending, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Saves pending messages, commits the session's transaction and wakes the dispatcher.</summary>
    public static async Task CommitAsync(this IOutbox outbox, IClientSessionHandle session, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        await outbox.SaveAsync(session, cancellationToken).ConfigureAwait(false);
        await session.CommitTransactionAsync(cancellationToken).ConfigureAwait(false);
        Buffer(outbox).Services.GetService<IDispatchSignal>()?.Notify();
    }

    private static IOutboxSession Buffer(IOutbox outbox) =>
        outbox as IOutboxSession
            ?? throw new ArgumentException("Pass the IOutbox resolved from the container.", nameof(outbox));
}
