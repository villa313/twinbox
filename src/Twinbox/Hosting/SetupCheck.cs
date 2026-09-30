using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Messaging;
using Twinbox.Migration;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Hosting;

/// <summary>Stops the host at startup, with the fix spelled out, when Twinbox is missing a piece it needs.</summary>
internal sealed class SetupCheck(IServiceProvider services, RouteTable routes, IOptions<TwinboxOptions> options) : IHostedService
{
    public const string WriteOnlyHint =
        "If another app dispatches this app's messages, set Twinbox:Dispatcher:Enabled to false and name the transport in each route instead.";

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var problems = FindProblems();
        return problems.Count switch
        {
            0 => Task.CompletedTask,
            1 => throw new InvalidOperationException(problems[0]),
            _ => throw new InvalidOperationException(
                "Twinbox isn't set up correctly:" + string.Concat(problems.Select(p => $"{Environment.NewLine}- {p}"))),
        };
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public IReadOnlyList<string> FindProblems()
    {
        var problems = new List<string>();
        if (!IsRegistered<IOutboxStore>())
        {
            problems.Add(SetupMessages.NoOutboxStore);
        }
        else if (services.GetService<InboxSeedOptions>() is not null && !IsRegistered<IInboxStore>())
        {
            problems.Add(InboxSeedService.NoInboxStore);
        }

        // A disabled dispatcher means another host sends, so only routes that rely on a local default transport matter.
        var dispatching = options.Value.Dispatcher.Enabled;
        var allRoutes = routes.All.ToArray();
        if (!IsRegistered<ITransport>())
        {
            if (dispatching || allRoutes.Any(r => r.Route.Transport is null))
            {
                problems.Add($"{SetupMessages.NoTransport} {WriteOnlyHint}");
            }

            return problems;
        }

        if (dispatching)
        {
            var transports = services.GetRequiredService<TransportRegistry>();
            foreach (var (messageType, route) in allRoutes)
            {
                if (route.Transport is { } name && !transports.TryGet(name, out _))
                {
                    problems.Add(
                        $"Route<{messageType.Name}>().To(\"{route.Destination}\", transport: \"{name}\") can't be delivered. "
                        + SetupMessages.UnknownTransport(name, transports.Names));
                }
            }
        }

        return problems;
    }

    /// <summary>Asks the container instead of resolving, so no store or broker client is built just to be counted.</summary>
    private bool IsRegistered<T>()
        where T : class =>
        services.GetService<IServiceProviderIsService>() is { } lookup
            ? lookup.IsService(typeof(T))
            : services.GetServices<T>().Any();
}
