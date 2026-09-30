using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Twinbox.Aspire;
using Twinbox.Diagnostics;

namespace Microsoft.Extensions.Hosting;

public static class TwinboxServiceDefaultsExtensions
{
    /// <summary>Adds Twinbox's trace source, meter and "ready" health check; exporters stay with ServiceDefaults.
    /// Repeat calls are ignored.</summary>
    public static TBuilder AddTwinboxServiceDefaults<TBuilder>(this TBuilder builder, Action<TwinboxServiceDefaultsOptions>? configure = null)
        where TBuilder : IHostApplicationBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        if (services.Any(d => d.ServiceType == typeof(TwinboxServiceDefaultsMarker)))
        {
            return builder;
        }

        services.AddSingleton<TwinboxServiceDefaultsMarker>();
        var options = new TwinboxServiceDefaultsOptions();
        configure?.Invoke(options);

        services.AddOpenTelemetry()
            .WithTracing(tracing => tracing.AddSource(TwinboxDiagnostics.SourceName))
            .WithMetrics(metrics => metrics.AddMeter(TwinboxDiagnostics.SourceName));

        services.AddHealthChecks().AddTwinbox(
            options.HealthCheckName,
            options.MaxPendingAge,
            options.MaxDeadMessages,
            tags: options.IncludeInLiveness ? ["ready", "live"] : ["ready"]);
        return builder;
    }
}
