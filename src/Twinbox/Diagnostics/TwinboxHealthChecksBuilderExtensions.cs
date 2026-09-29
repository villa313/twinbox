using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Twinbox.Diagnostics;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Microsoft.Extensions.DependencyInjection;

public static class TwinboxHealthChecksBuilderExtensions
{
    /// <summary>Reports <paramref name="failureStatus"/> when the backlog is stale or messages are dead-lettered.</summary>
    public static IHealthChecksBuilder AddTwinbox(
        this IHealthChecksBuilder builder,
        string name = "twinbox",
        TimeSpan? maxPendingAge = null,
        long maxDeadMessages = 0,
        HealthStatus failureStatus = HealthStatus.Degraded,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new TwinboxHealthCheck(
                sp.GetServices<IOutboxStore>(),
                sp.GetRequiredService<TenantDirectory>(),
                sp.GetRequiredService<TimeProvider>(),
                maxPendingAge ?? TimeSpan.FromMinutes(5),
                maxDeadMessages),
            failureStatus,
            tags));
    }
}
