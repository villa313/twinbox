using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Twinbox.UnitTests;

public sealed class HealthCheckTests
{
    [Fact]
    public async Task StaleBacklog_ReportsDegraded()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders"),
            s => s.AddHealthChecks().AddTwinbox(maxPendingAge: TimeSpan.FromMinutes(1)));
        var health = host.Services.GetRequiredService<HealthCheckService>();

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        Assert.Equal(HealthStatus.Healthy, (await health.CheckHealthAsync()).Status);

        host.Time.Advance(TimeSpan.FromMinutes(2));
        Assert.Equal(HealthStatus.Degraded, (await health.CheckHealthAsync()).Status);
    }

    [Fact]
    public async Task DelayedSend_OnlyAgesOnceDue()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders"),
            s => s.AddHealthChecks().AddTwinbox(maxPendingAge: TimeSpan.FromMinutes(1)));
        var health = host.Services.GetRequiredService<HealthCheckService>();

        await host.SendAsync(o => o.Send(new OrderPlaced(1), new SendOptions { Delay = TimeSpan.FromHours(1) }));
        host.Time.Advance(TimeSpan.FromMinutes(59));
        Assert.Equal(HealthStatus.Healthy, (await health.CheckHealthAsync()).Status);

        host.Time.Advance(TimeSpan.FromMinutes(3));
        Assert.Equal(HealthStatus.Degraded, (await health.CheckHealthAsync()).Status);
    }

    [Fact]
    public async Task DeadLetteredMessages_ReportDegraded()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders"),
            s => s.AddHealthChecks().AddTwinbox());
        host.Harness.Transport.OnSend = _ => throw new Transport.PermanentDeliveryException("bad");

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        var report = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync();
        Assert.Equal(HealthStatus.Degraded, report.Status);
    }
}
