using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Messaging;
using Twinbox.Storage;

namespace Twinbox;

public static class TransactionalOutboxExtensions
{
    /// <summary>Writes the messages sent so far through <paramref name="transaction"/>; they're dispatched once it commits.</summary>
    public static async Task SaveAsync(this IOutbox outbox, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var buffer = Buffer(outbox);
        var pending = buffer.TakePending();
        if (pending.Count == 0)
        {
            return;
        }

        var writer = buffer.Services.GetService<IOutboxTransactionWriter>()
            ?? throw new InvalidOperationException(
                "Saving outbox messages through a DbTransaction needs an ADO.NET store, and none is registered. Install Twinbox.SqlServer, "
                + "Twinbox.PostgreSql, Twinbox.MySql or Twinbox.Oracle and call UseSqlServer(...), UsePostgreSql(...), UseMySql(...) or UseOracle(...) "
                + "inside AddTwinbox. With Twinbox.EntityFrameworkCore, save through the DbContext (SaveChanges) instead; with Twinbox.MongoDB, "
                + "use outbox.CommitAsync(session).");
        await writer.WriteAsync(transaction, pending, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Saves pending messages, commits <paramref name="transaction"/> and wakes the dispatcher.</summary>
    public static async Task CommitAsync(this IOutbox outbox, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        await outbox.SaveAsync(transaction, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        Buffer(outbox).Services.GetService<IDispatchSignal>()?.Notify();
    }

    private static OutboxBuffer Buffer(IOutbox outbox) =>
        outbox as OutboxBuffer
            ?? throw new ArgumentException("Pass the IOutbox resolved from the container.", nameof(outbox));
}
