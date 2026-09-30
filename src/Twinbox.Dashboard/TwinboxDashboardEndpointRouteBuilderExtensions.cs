using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Twinbox.Dashboard;

namespace Microsoft.AspNetCore.Builder;

public static class TwinboxDashboardEndpointRouteBuilderExtensions
{
    /// <summary>Serves the Twinbox dashboard and its JSON API under <paramref name="prefix"/>. Every request is refused until an
    /// authorization policy is attached, e.g. <c>.RequireAuthorization("ops")</c>, unless AllowAnonymous is set.</summary>
    public static IEndpointConventionBuilder MapTwinboxDashboard(
        this IEndpointRouteBuilder endpoints,
        string prefix = "/twinbox",
        Action<TwinboxDashboardOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        if (!prefix.StartsWith('/') || prefix.AsSpan().IndexOfAny("{}?#") >= 0)
        {
            throw new ArgumentException("The prefix must be a literal path starting with '/', such as \"/twinbox\".", nameof(prefix));
        }

        var options = new TwinboxDashboardOptions();
        configure?.Invoke(options);
        prefix = prefix.TrimEnd('/');

        var services = endpoints.ServiceProvider;
        var logger = services.GetService<ILoggerFactory>()?.CreateLogger("Twinbox.Dashboard") ?? NullLogger.Instance;
        var dashboard = new DashboardEndpoints(
            logger,
            options,
            new DashboardStores(services),
            new DashboardCsrf(services.GetService<IDataProtectionProvider>()),
            services.GetService<TimeProvider>() ?? TimeProvider.System,
            prefix);
        dashboard.WarnIfAnonymous();

        var group = endpoints.MapGroup(prefix);
        group.MapGet("/", dashboard.Guard(dashboard.PageAsync));
        group.MapGet("/api/config", dashboard.Guard(dashboard.ConfigAsync));
        group.MapGet("/api/stats", dashboard.Guard(dashboard.StatsAsync));
        group.MapGet("/api/messages", dashboard.Guard(dashboard.MessagesAsync));
        group.MapGet("/api/messages/{id}", dashboard.Guard(dashboard.MessageAsync));
        group.MapPost("/api/messages/replay", dashboard.Guard(dashboard.ReplayAsync, mutates: true));
        group.MapPost("/api/messages/delete", dashboard.Guard(dashboard.DeleteAsync, mutates: true));
        group.MapPost("/api/dead/replay-all", dashboard.Guard(dashboard.ReplayAllDeadAsync, mutates: true));
        return group;
    }
}
