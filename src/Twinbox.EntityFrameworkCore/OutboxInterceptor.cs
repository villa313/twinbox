using System.Data.Common;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore;

/// <summary>Adds buffered outbox messages to every SaveChanges and wakes the dispatcher once they commit.</summary>
internal sealed class OutboxInterceptor : ISaveChangesInterceptor, IDbTransactionInterceptor
{
    public static readonly OutboxInterceptor Instance = new();

    private static readonly ConditionalWeakTable<DbContext, object> AwaitingCommit = [];

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

    private static void Flush(DbContext? context)
    {
        if (context is null || ApplicationServices(context) is not { } services)
        {
            return;
        }

        var session = TryResolve<IOutboxSession>(services);
        if (session is not { HasPending: true })
        {
            return;
        }

        context.Set<OutboxMessage>().AddRange(session.TakePending());
        AwaitingCommit.AddOrUpdate(context, services);
    }

    private static void NotifyIfCommitted(DbContext? context)
    {
        if (context?.Database.CurrentTransaction is null)
        {
            Notify(context);
        }
    }

    private static void Notify(DbContext? context)
    {
        if (context is not null && AwaitingCommit.TryGetValue(context, out var services) && AwaitingCommit.Remove(context))
        {
            TryResolve<IDispatchSignal>((IServiceProvider)services)?.Notify();
        }
    }

    private static IServiceProvider? ApplicationServices(DbContext context) =>
        context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.ApplicationServiceProvider;

    private static T? TryResolve<T>(IServiceProvider services)
        where T : class
    {
        try
        {
            return services.GetService<T>();
        }
        catch (InvalidOperationException)
        {
            // Pooled contexts carry the root provider, which can't hand out scoped services.
            return null;
        }
    }
}
