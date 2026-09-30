using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore;

internal static class OutboxEnlistment
{
    private static readonly ConditionalWeakTable<DbContext, IOutboxSession> Sessions = [];

    public static void Enlist(DbContext context, IOutboxSession session) => Sessions.AddOrUpdate(context, session);

    public static IOutboxSession? Find(DbContext context)
    {
        if (Sessions.TryGetValue(context, out var session))
        {
            return session;
        }

        var services = context.GetService<IDbContextOptions>().FindExtension<CoreOptionsExtension>()?.ApplicationServiceProvider;
        return services is null ? null : TryResolve(services);
    }

    public static void EnlistFromScope(DbContext context, IServiceProvider scopedServices)
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
