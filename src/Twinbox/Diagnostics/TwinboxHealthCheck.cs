using Microsoft.Extensions.Diagnostics.HealthChecks;
using Twinbox.Storage;

namespace Twinbox.Diagnostics;

internal sealed class TwinboxHealthCheck(IOutboxStore store, TimeProvider time, TimeSpan maxPendingAge, long maxDeadMessages) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var stats = await store.GetStatisticsAsync(cancellationToken).ConfigureAwait(false);
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
