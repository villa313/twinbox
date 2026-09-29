using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox;
using Twinbox.Dispatch;
using Twinbox.Hosting;
using Twinbox.Inbox;
using Twinbox.Messaging;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Tenancy;
using Twinbox.Transport;

namespace Microsoft.Extensions.DependencyInjection;

public static class TwinboxServiceCollectionExtensions
{
    public static IServiceCollection AddTwinbox(this IServiceCollection services, Action<TwinboxBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new TwinboxBuilder(services);
        services.AddSingleton(builder.Routes);
        services.AddSingleton(builder.MessageTypes);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IMessageSerializer>(new SystemTextJsonMessageSerializer());
        services.TryAddSingleton<IMessageIdGenerator, Uuid7MessageIdGenerator>();
        services.TryAddSingleton<TransportRegistry>();
        services.TryAddSingleton<TwinboxScopeFactory>();
        services.TryAddSingleton<TenantDirectory>();
        services.TryAddSingleton<MessagePreparer>();
        services.TryAddSingleton<HandlerRegistry>();
        services.TryAddSingleton<IInboundPipeline, InboundPipeline>();
        services.TryAddSingleton<DispatchSignal>();
        services.TryAddSingleton<IDispatchSignal>(sp => sp.GetRequiredService<DispatchSignal>());
        services.TryAddSingleton<IOutboxDispatcher, OutboxDispatcher>();

        services.TryAddScoped<OutboxBuffer>();
        services.TryAddScoped<HandlerTransaction>();
        services.TryAddScoped<IOutbox>(sp => sp.GetRequiredService<OutboxBuffer>());
        services.TryAddScoped<IOutboxSession>(sp => sp.GetRequiredService<OutboxBuffer>());

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
}
