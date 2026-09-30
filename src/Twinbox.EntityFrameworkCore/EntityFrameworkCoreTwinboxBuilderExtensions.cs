using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Twinbox.EntityFrameworkCore;
using Twinbox.Storage;
#if !NET9_0_OR_GREATER
using Microsoft.EntityFrameworkCore.Infrastructure;
#endif

namespace Twinbox;

public static class EntityFrameworkCoreTwinboxBuilderExtensions
{
    /// <summary>
    /// Stores the outbox and inbox in <typeparamref name="TContext"/>'s database. Messages sent through
    /// <see cref="IOutbox"/> are saved by the next SaveChanges of a context resolved in the same scope, pooled or not.
    /// The first registered context also hosts the inbox. Call after AddDbContext.
    /// </summary>
    public static TwinboxBuilder UseEntityFrameworkCore<TContext>(this TwinboxBuilder builder)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        // Each context gets its own store, so a modular monolith dispatches every module's outbox table.
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxStore, EntityFrameworkOutboxStore<TContext>>());
        builder.Services.TryAddSingleton<IInboxStore, EntityFrameworkInboxStore<TContext>>();
        AddInterceptor<TContext>(builder.Services);
        EnlistResolvedContexts<TContext>(builder.Services);
        return builder;
    }

    /// <summary>Wraps the context's registration so every instance is tied to the outbox of the scope resolving it.</summary>
    private static void EnlistResolvedContexts<TContext>(IServiceCollection services)
        where TContext : DbContext
    {
        var registration = services.LastOrDefault(d => d.ServiceType == typeof(TContext) && !d.IsKeyedService)
            ?? throw new InvalidOperationException(NotRegistered<TContext>());

        services.Add(new ServiceDescriptor(
            typeof(TContext),
            sp =>
            {
                var context = (TContext)Create(registration, sp);
                OutboxEnlistment.EnlistFromScope(context, sp);
                return context;
            },
            registration.Lifetime));
    }

    private static string NotRegistered<TContext>() =>
        $"{typeof(TContext).Name} isn't registered with the service collection. Call services.AddDbContext<{typeof(TContext).Name}>(...) "
        + $"or AddDbContextPool<{typeof(TContext).Name}>(...) before UseEntityFrameworkCore<{typeof(TContext).Name}>().";

    private static object Create(ServiceDescriptor registration, IServiceProvider services) =>
        registration.ImplementationInstance
            ?? registration.ImplementationFactory?.Invoke(services)
            ?? ActivatorUtilities.CreateInstance(services, registration.ImplementationType!);

#if NET9_0_OR_GREATER
    private static void AddInterceptor<TContext>(IServiceCollection services)
        where TContext : DbContext =>
        services.ConfigureDbContext<TContext>(options => options.AddInterceptors(OutboxInterceptor.Instance));
#else
    private static void AddInterceptor<TContext>(IServiceCollection services)
        where TContext : DbContext
    {
        // EF Core 8 has no ConfigureDbContext, so decorate the options registration AddDbContext made.
        var registration = services.LastOrDefault(d => d.ServiceType == typeof(DbContextOptions<TContext>))
            ?? throw new InvalidOperationException(NotRegistered<TContext>());

        services.Add(new ServiceDescriptor(
            typeof(DbContextOptions<TContext>),
            sp => WithInterceptor((DbContextOptions<TContext>)Create(registration, sp)),
            registration.Lifetime));
    }


    private static DbContextOptions<TContext> WithInterceptor<TContext>(DbContextOptions<TContext> options)
        where TContext : DbContext
    {
        var core = options.FindExtension<CoreOptionsExtension>() ?? new CoreOptionsExtension();
        var interceptors = core.Interceptors ?? [];
        return interceptors.Contains(OutboxInterceptor.Instance)
            ? options
            : (DbContextOptions<TContext>)options.WithExtension(core.WithInterceptors([.. interceptors, OutboxInterceptor.Instance]));
    }
#endif
}
