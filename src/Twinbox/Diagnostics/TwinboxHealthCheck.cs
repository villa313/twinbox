using Microsoft.Extensions.Diagnostics.HealthChecks;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.Diagnostics;

internal sealed class TwinboxHealthCheck(IEnumerable<IOutboxStore> stores, TenantDirectory tenants, TimeProvider time, TimeSpan maxPendingAge, long maxDeadMessages) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var all = new List<OutboxStatistics>();
        foreach (var tenant in await tenants.GetTenantsAsync(cancellationToken).ConfigureAwait(false))
        {
            using var _ = TenantScope.Enter(tenant);
            foreach (var store in stores)
            {
                all.Add(await store.GetStatisticsAsync(cancellationToken).ConfigureAwait(false));
            }
        }

        var stats = new OutboxStatistics(
            all.Sum(s => s.PendingCount),
            all.Min(s => s.OldestPendingCreatedAt),
            all.Sum(s => s.DeadCount));
        var data = new Dictionary<string, object>
        {
            ["pending"] = stats.PendingCount,
            ["dead"] = stats.DeadCount,
        };

        if (stats.OldestPendingCreatedAt is { } oldest && time.GetUtcNow() - oldest > maxPendingAge)
        {
            return new HealthCheckResult(context.Registration.FailureStatus, $"Oldest pending outbox message is older than {maxPendingAge}.", data: data);
        }

        return stats.DeadCount > maxDeadMessages
            ? new HealthCheckResult(context.Registration.FailureStatus, $"{stats.DeadCount} outbox message(s) are dead-lettered.", data: data)
            : HealthCheckResult.Healthy(data: data);
    }
}
