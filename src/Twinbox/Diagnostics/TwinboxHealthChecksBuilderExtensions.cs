using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Twinbox.Diagnostics;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Microsoft.Extensions.DependencyInjection;

public static class TwinboxHealthChecksBuilderExtensions
{
    /// <summary>
    /// Reports <paramref name="failureStatus"/> when the backlog is older than <paramref name="maxPendingAge"/>, and
    /// <paramref name="deadLetterStatus"/> (default: the same) when more than <paramref name="maxDeadMessages"/> are dead.
    /// </summary>
    public static IHealthChecksBuilder AddTwinbox(
        this IHealthChecksBuilder builder,
        string name = "twinbox",
        TimeSpan? maxPendingAge = null,
        long maxDeadMessages = 0,
        HealthStatus failureStatus = HealthStatus.Degraded,
        IEnumerable<string>? tags = null,
        HealthStatus? deadLetterStatus = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new TwinboxHealthCheck(
                sp.GetServices<IOutboxStore>(),
                sp.GetService<TenantDirectory>() ?? throw new InvalidOperationException(
                    "The Twinbox health check needs Twinbox itself: call services.AddTwinbox(twinbox => ...) with a store, e.g. UseEntityFrameworkCore<TContext>()."),
                sp.GetRequiredService<TimeProvider>(),
                maxPendingAge ?? TimeSpan.FromMinutes(5),
                maxDeadMessages,
                deadLetterStatus),
            failureStatus,
            tags));
    }
}
