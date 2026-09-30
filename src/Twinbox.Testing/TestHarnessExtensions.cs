using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Twinbox.InMemory;
using Twinbox.Serialization;
using Twinbox.Testing;
using Twinbox.Transport;

namespace Twinbox;

public static class TestHarnessExtensions
{
    /// <summary>In-memory store and transport with background work disabled; drive it with <see cref="TwinboxTestHarness.DrainAsync"/>.</summary>
    public static TwinboxBuilder UseTestHarness(this TwinboxBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.UseInMemory(o => o.AutoDeliver = false)
            .Configure(o => o.Dispatcher.Enabled = false);
        builder.Services.TryAddSingleton(sp => new TwinboxTestHarness(
            sp.GetRequiredService<IOutboxDispatcher>(),
            sp.GetRequiredService<IInboundPipeline>(),
            sp.GetRequiredService<InMemoryOutboxStore>(),
            sp.GetRequiredService<InMemoryTransport>(),
            sp.GetRequiredService<IMessageNames>(),
            sp.GetRequiredService<IMessageSerializer>()));
        return builder;
    }

    public static TwinboxTestHarness GetTwinboxHarness(this IServiceProvider services) =>
        services.GetRequiredService<TwinboxTestHarness>();
}
