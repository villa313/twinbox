using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace Twinbox.Testing;

/// <summary>Finds message types a base-type registration was meant to cover but doesn't, before a send or delivery fails.</summary>
public static class TopologyChecks
{
    /// <summary>Concrete subtypes of <typeparamref name="TBase"/> that have no route, so sending one would throw.</summary>
    [RequiresUnreferencedCode("Scans assemblies for subtypes.")]
    public static IReadOnlyList<Type> FindUnroutedSubtypes<TBase>(this IServiceProvider services, params Assembly[] assemblies)
        where TBase : class
    {
        var topology = Topology(services);
        return [.. Subtypes<TBase>(assemblies).Where(type => topology.DestinationsOf(type).Count == 0)];
    }

    /// <summary>Concrete subtypes of <typeparamref name="TBase"/> that an incoming message couldn't be matched to or has no handler for.</summary>
    [RequiresUnreferencedCode("Scans assemblies for subtypes.")]
    public static IReadOnlyList<Type> FindUnhandledSubtypes<TBase>(this IServiceProvider services, params Assembly[] assemblies)
        where TBase : class
    {
        var topology = Topology(services);
        return [.. Subtypes<TBase>(assemblies).Where(type => !topology.IsKnown(type) || topology.ConsumersOf(type).Count == 0)];
    }

    private static ITwinboxTopology Topology(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.GetRequiredService<ITwinboxTopology>();
    }

    [RequiresUnreferencedCode("Scans assemblies for subtypes.")]
    private static IEnumerable<Type> Subtypes<TBase>(Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);
        return (assemblies.Length == 0 ? [typeof(TBase).Assembly] : assemblies)
            .SelectMany(assembly => assembly.GetTypes())
            .Where(type => type is { IsClass: true, IsAbstract: false, ContainsGenericParameters: false } && typeof(TBase).IsAssignableFrom(type));
    }
}
