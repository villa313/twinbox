using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox;
using Twinbox.Dispatch;
using Twinbox.Hosting;
using Twinbox.Inbox;
using Twinbox.Maintenance;
using Twinbox.Messaging;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Tenancy;
using Twinbox.Transport;

namespace Microsoft.Extensions.DependencyInjection;

public static class TwinboxServiceCollectionExtensions
{
    /// <summary>Runs the dispatcher in this host even where shared setup called <see cref="TwinboxBuilder.SendOnly"/>, whatever the registration order.</summary>
    public static IServiceCollection EnableTwinboxDispatcher(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.PostConfigure<TwinboxOptions>(options => options.Dispatcher.Enabled = true);
    }

    public static IServiceCollection AddTwinbox(this IServiceCollection services, Action<TwinboxBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // Later calls extend the first one, so modules can each add their own routes and handlers.
        var routes = Existing<RouteTable>(services);
        var messageTypes = Existing<MessageTypeRegistry>(services);
        var builder = new TwinboxBuilder(services, routes ?? new RouteTable(), messageTypes ?? new MessageTypeRegistry());
        if (routes is null)
        {
            services.AddSingleton(builder.Routes);
            services.AddSingleton(builder.MessageTypes);
            services.AddSingleton<IMessageNames>(builder.MessageTypes);
        }

        services.TryAddSingleton(TimeProvider.System);
        // Lazy, so Native AOT apps that replace it never construct the reflection-based default, which throws there.
        services.TryAddSingleton<IMessageSerializer>(_ => new SystemTextJsonMessageSerializer());
        services.TryAddSingleton<IMessageIdGenerator, Uuid7MessageIdGenerator>();
        services.TryAddSingleton<TransportRegistry>();
        services.TryAddSingleton<HeaderProfiles>();
        services.TryAddSingleton<TwinboxScopeFactory>();
        services.TryAddSingleton<TenantDirectory>();
        services.TryAddSingleton<MessagePreparer>();
        services.TryAddSingleton<HandlerRegistry>();
        services.TryAddSingleton<ITwinboxTopology, TwinboxTopology>();
        services.TryAddSingleton<IInboundPipeline, InboundPipeline>();
        services.TryAddSingleton<DispatchSignal>();
        services.TryAddSingleton<IDispatchSignal>(sp => sp.GetRequiredService<DispatchSignal>());
        services.TryAddSingleton<IOutboxDispatcher, OutboxDispatcher>();
        services.TryAddSingleton<ITwinboxMaintenance, TwinboxMaintenance>();

        services.TryAddScoped<OutboxBuffer>();
        services.TryAddScoped<HandlerTransaction>();
        services.TryAddScoped<IOutbox>(sp => sp.GetRequiredService<OutboxBuffer>());
        services.TryAddScoped<IOutboxSession>(sp => sp.GetRequiredService<OutboxBuffer>());

        // Ahead of the dispatcher, so a missing store or transport stops the host before anything is dispatched.
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SetupCheck>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxDispatcherService>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RetentionService>());

        configure(builder);

        // Registered after code configuration so appsettings wins, letting ops tune without a redeploy.
        services.AddOptions<TwinboxOptions>()
            .Configure<IServiceProvider>((options, sp) =>
                sp.GetService<IConfiguration>()?.GetSection(TwinboxOptions.SectionName).Bind(options))
            .Validate(o => o.Dispatcher.BatchSize > 0, "Twinbox:Dispatcher:BatchSize must be positive.")
            .Validate(o => o.Retry.MaxAttempts > 0, "Twinbox:Retry:MaxAttempts must be positive.")
            .Validate(o => !string.IsNullOrWhiteSpace(o.InstanceId), "Twinbox:InstanceId must be set.");

        return services;
    }

    private static T? Existing<T>(IServiceCollection services)
        where T : class =>
        services.LastOrDefault(d => d.ServiceType == typeof(T))?.ImplementationInstance as T;
}
