using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.InMemory;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox;

public static class InMemoryTwinboxBuilderExtensions
{
    /// <summary>Uses in-memory storage and an in-memory loopback transport. Nothing survives a restart.</summary>
    public static TwinboxBuilder UseInMemory(this TwinboxBuilder builder, Action<InMemoryTransportOptions>? configure = null) =>
        builder.UseInMemoryStore().UseInMemoryTransport(configure);

    public static TwinboxBuilder UseInMemoryStore(this TwinboxBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<InMemoryOutboxStore>();
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxStore, InMemoryOutboxStore>(sp => sp.GetRequiredService<InMemoryOutboxStore>()));
        builder.Services.TryAddSingleton<InMemoryInboxStore>();
        builder.Services.TryAddSingleton<IInboxStore>(sp => sp.GetRequiredService<InMemoryInboxStore>());
        builder.Services.TryAddScoped<InMemoryUnitOfWork>();
        return builder;
    }

    public static TwinboxBuilder UseInMemoryTransport(this TwinboxBuilder builder, Action<InMemoryTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = builder.Services.AddOptions<InMemoryTransportOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        builder.Services.TryAddSingleton<InMemoryTransport>();
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, InMemoryTransport>(sp => sp.GetRequiredService<InMemoryTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, InMemoryReceiverService>());
        return builder;
    }
}
