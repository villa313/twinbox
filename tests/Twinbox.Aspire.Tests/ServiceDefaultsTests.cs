using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Twinbox.Diagnostics;

namespace Twinbox.Aspire.Tests;

public sealed class ServiceDefaultsTests
{
    [Fact]
    public void AddTwinboxServiceDefaults_RegistersReadyHealthCheck()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new());

        builder.AddTwinboxServiceDefaults();

        using var provider = builder.Services.BuildServiceProvider();
        var registration = Assert.Single(provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations);
        Assert.Equal("twinbox", registration.Name);
        Assert.Equal(["ready"], registration.Tags);
        Assert.Equal(HealthStatus.Degraded, registration.FailureStatus);
    }

    [Fact]
    public void AddTwinboxServiceDefaults_CalledTwice_RegistersEverythingOnce()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new());
        builder.AddTwinboxServiceDefaults();
        var count = builder.Services.Count;

        builder.AddTwinboxServiceDefaults(o => o.HealthCheckName = "other");

        Assert.Equal(count, builder.Services.Count);
        using var provider = builder.Services.BuildServiceProvider();
        Assert.Equal("twinbox", Assert.Single(provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations).Name);
    }

    [Fact]
    public void AddTwinboxServiceDefaults_IncludeInLiveness_AddsLiveTag()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new());

        builder.AddTwinboxServiceDefaults(o =>
        {
            o.HealthCheckName = "outbox";
            o.IncludeInLiveness = true;
        });

        using var provider = builder.Services.BuildServiceProvider();
        var registration = Assert.Single(provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations);
        Assert.Equal("outbox", registration.Name);
        Assert.Equal(["live", "ready"], registration.Tags.Order());
    }

    [Fact]
    public void AddTwinboxServiceDefaults_ListensToTwinboxTraceSource()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new());
        builder.AddTwinboxServiceDefaults();
        builder.AddTwinboxServiceDefaults();

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredService<TracerProvider>();
        using var source = new ActivitySource(TwinboxDiagnostics.SourceName);
        using var activity = source.StartActivity("probe");

        Assert.NotNull(activity);
        Assert.True(activity.Recorded);
    }

    [Fact]
    public void AddTwinboxServiceDefaults_ListensToTwinboxMeter()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new());
        builder.AddTwinboxServiceDefaults();
        builder.Services.ConfigureOpenTelemetryMeterProvider(m => m.AddReader(new BaseExportingMetricReader(new DiscardingExporter())));

        using var provider = builder.Services.BuildServiceProvider();
        _ = provider.GetRequiredService<MeterProvider>();
        using var meter = new Meter(TwinboxDiagnostics.SourceName);
        var counter = meter.CreateCounter<long>("twinbox.test.probe");

        Assert.True(counter.Enabled);
    }

    private sealed class DiscardingExporter : BaseExporter<Metric>
    {
        public override ExportResult Export(in Batch<Metric> batch) => ExportResult.Success;
    }
}
