using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Twinbox;
using Twinbox.EntityFrameworkCore;
using Twinbox.Storage;

namespace Microsoft.EntityFrameworkCore;

public static class OutboxEnlistment
{
    private static readonly ConditionalWeakTable<DbContext, IOutboxSession> Sessions = [];

    /// <summary>
    /// Makes <paramref name="context"/> save the messages sent through <paramref name="outbox"/>. Contexts resolved
    /// from DI are enlisted automatically; call this for contexts you create yourself.
    /// </summary>
    public static TContext EnlistOutbox<TContext>(this TContext context, IOutbox outbox)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(context);
        var session = outbox as IOutboxSession
            ?? throw new ArgumentException("Pass the IOutbox resolved from the container.", nameof(outbox));
        Sessions.AddOrUpdate(context, session);

        // A context built by hand may lack Twinbox's interceptor, so hook its own events; flushing twice is harmless.
        context.SavingChanges += (_, _) => OutboxInterceptor.Flush(context);
        context.SavedChanges += (_, _) => OutboxInterceptor.NotifyIfCommitted(context);
        return context;
    }

    internal static IOutboxSession? Find(DbContext context)
    {
        if (Sessions.TryGetValue(context, out var session))
        {
            return session;
        }

        var services = context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.ApplicationServiceProvider;
        return services is null ? null : TryResolve(services);
    }

    internal static void EnlistFromScope(DbContext context, IServiceProvider scopedServices)
    {
        if (TryResolve(scopedServices) is { } session)
        {
            Sessions.AddOrUpdate(context, session);
        }
    }

    private static IOutboxSession? TryResolve(IServiceProvider services)
    {
        try
        {
            return services.GetService<IOutboxSession>();
        }
        catch (InvalidOperationException)
        {
            // A root provider (pooled contexts, singletons) can't hand out the scoped session.
            return null;
        }
    }
}
