using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Twinbox.Diagnostics;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Microsoft.Extensions.DependencyInjection;

public static class TwinboxHealthChecksBuilderExtensions
{
    private static readonly TimeSpan DefaultMaxPendingAge = TimeSpan.FromMinutes(5);

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
        return builder.Add(Registration(name, maxPendingAge ?? DefaultMaxPendingAge, maxDeadMessages, failureStatus, tags, deadLetterStatus));
    }

    /// <summary>Reports <paramref name="failureStatus"/> when the oldest pending message has waited longer than <paramref name="maxPendingAge"/> (default 5 minutes).</summary>
    /// <remarks>A backlog usually drains by itself once a broker recovers, hence the Degraded default.</remarks>
    public static IHealthChecksBuilder AddTwinboxBacklog(
        this IHealthChecksBuilder builder,
        string name = "twinbox-backlog",
        TimeSpan? maxPendingAge = null,
        HealthStatus failureStatus = HealthStatus.Degraded,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Add(Registration(name, maxPendingAge ?? DefaultMaxPendingAge, maxDeadMessages: null, failureStatus, tags, deadLetterStatus: null));
    }

    /// <summary>Reports <paramref name="failureStatus"/> when more than <paramref name="maxDeadMessages"/> messages are dead-lettered.</summary>
    /// <remarks>Dead letters never recover on their own; someone has to requeue or discard them, hence the Unhealthy default.</remarks>
    public static IHealthChecksBuilder AddTwinboxDeadLetters(
        this IHealthChecksBuilder builder,
        string name = "twinbox-dead-letters",
        long maxDeadMessages = 0,
        HealthStatus failureStatus = HealthStatus.Unhealthy,
        IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Add(Registration(name, maxPendingAge: null, maxDeadMessages, failureStatus, tags, deadLetterStatus: null));
    }

    private static HealthCheckRegistration Registration(
        string name,
        TimeSpan? maxPendingAge,
        long? maxDeadMessages,
        HealthStatus failureStatus,
        IEnumerable<string>? tags,
        HealthStatus? deadLetterStatus) =>
        new(
            name,
            sp => new TwinboxHealthCheck(
                sp.GetServices<IOutboxStore>(),
                sp.GetService<TenantDirectory>() ?? throw new InvalidOperationException(
                    "The Twinbox health check needs Twinbox itself: call services.AddTwinbox(twinbox => ...) with a store, e.g. UseEntityFrameworkCore<TContext>()."),
                sp.GetRequiredService<TimeProvider>(),
                maxPendingAge,
                maxDeadMessages,
                deadLetterStatus),
            failureStatus,
            tags);
}
