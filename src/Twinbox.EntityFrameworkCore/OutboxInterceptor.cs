using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore;

/// <summary>Adds buffered outbox messages to every SaveChanges and wakes the dispatcher once they commit.</summary>
internal sealed class OutboxInterceptor : ISaveChangesInterceptor, IDbTransactionInterceptor
{
    public static readonly OutboxInterceptor Instance = new();

    private static readonly ConditionalWeakTable<DbContext, IOutboxSession> AwaitingCommit = [];

    private OutboxInterceptor()
    {
    }

    public InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        Flush(eventData.Context);
        return result;
    }

    public ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        Flush(eventData.Context);
        return ValueTask.FromResult(result);
    }

    public int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        NotifyIfCommitted(eventData.Context);
        return result;
    }

    public ValueTask<int> SavedChangesAsync(SaveChangesCompletedEventData eventData, int result, CancellationToken cancellationToken = default)
    {
        NotifyIfCommitted(eventData.Context);
        return ValueTask.FromResult(result);
    }

    public void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) =>
        Notify(eventData.Context);

    public Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        Notify(eventData.Context);
        return Task.CompletedTask;
    }

    internal static void Flush(DbContext? context)
    {
        if (context is null || OutboxEnlistment.Find(context) is not { HasPending: true } session)
        {
            return;
        }

        context.TwinboxOutbox().AddRange(session.TakePending());
        AwaitingCommit.AddOrUpdate(context, session);
    }

    internal static void NotifyIfCommitted(DbContext? context)
    {
        if (context?.Database.CurrentTransaction is null)
        {
            Notify(context);
        }
    }

    private static void Notify(DbContext? context)
    {
        if (context is not null && AwaitingCommit.TryGetValue(context, out var session) && AwaitingCommit.Remove(context))
        {
            session.Services.GetService<IDispatchSignal>()?.Notify();
        }
    }
}
