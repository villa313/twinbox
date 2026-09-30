# .NET Aspire

Two packages, one for each side of an Aspire solution.

## Service defaults: `Twinbox.Aspire`

Add it to your ServiceDefaults project and call it from `AddServiceDefaults`:

```csharp
public static TBuilder AddServiceDefaults<TBuilder>(this TBuilder builder) where TBuilder : IHostApplicationBuilder
{
    builder.ConfigureOpenTelemetry();
    builder.AddDefaultHealthChecks();
    builder.AddTwinboxServiceDefaults(o => o.MaxDeadMessages = 10);
    // ...
    return builder;
}
```

It adds Twinbox's trace source and meter to OpenTelemetry and registers the Twinbox health check tagged `ready`, so it
shows up on `/health` but not on `/alive`. Exporters stay with your ServiceDefaults. Calling it twice is harmless.

| Option | Default | Description |
|---|---|---|
| `HealthCheckName` | `"twinbox"` | Name of the health check registration. |
| `IncludeInLiveness` | `false` | Also tag the check `live`. Off by default: a backlog shouldn't get the app restarted. |
| `MaxPendingAge` | `null` (5 minutes) | Report `Degraded` when the oldest pending message is older than this. |
| `MaxDeadMessages` | `0` | Report `Degraded` when more messages than this are dead. |

See [Observability](observability.md) for what the check and the telemetry contain.

## AppHost: `Twinbox.Aspire.Hosting`

Add it to the AppHost and point it at the dashboard each project maps with `MapTwinboxDashboard`:

```csharp
var builder = DistributedApplication.CreateBuilder(args);

builder.AddProject<Projects.Orders>("orders")
    .WithTwinboxDashboard("/twinbox");                           // same prefix as MapTwinboxDashboard

builder.AddProject<Projects.Billing>("billing")
    .WithTwinboxDashboard("/ops/twinbox", endpointName: "https");

builder.Build().Run();
```

This adds to the resource in the Aspire dashboard:

- a link to its Twinbox dashboard, through the first `https` endpoint (else `http`) unless `endpointName` is given;
- an **Open Twinbox dashboard** command (`twinbox-open-dashboard`), which starts a browser on the machine running the
  AppHost. Use the link when the Aspire dashboard is remote;
- a **Replay dead letters** command (`twinbox-replay-dead-letters`).

Both commands are disabled unless the resource is running.

### Replay dead letters

The command calls the dashboard's JSON API from the AppHost: it fetches `api/config` for the CSRF token and posts
`api/dead/replay-all` for every browsable store and tenant. The replayed count goes to the resource's console log.

The AppHost can't sign in, so the command only works when the dashboard doesn't need interactive authentication.
In development, map the dashboard with `AllowAnonymous` or a development-only policy:

```csharp
var dashboard = app.MapTwinboxDashboard("/twinbox", o => o.AllowAnonymous = app.Environment.IsDevelopment());
if (!app.Environment.IsDevelopment())
{
    dashboard.RequireAuthorization("ops");
}
```

`Twinbox.Aspire.Hosting` targets Aspire.Hosting 9.5 or later, which serves both .NET 8 and .NET 10 AppHosts.
