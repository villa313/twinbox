namespace Twinbox.Aspire;

public sealed class TwinboxServiceDefaultsOptions
{
    /// <summary>Name of the health check registration.</summary>
    public string HealthCheckName { get; set; } = "twinbox";

    /// <summary>Also tags the health check "live". Off by default: a backlog shouldn't get the app restarted.</summary>
    public bool IncludeInLiveness { get; set; }

    /// <summary>How old the oldest pending message may get before the check reports Degraded. Defaults to 5 minutes.</summary>
    public TimeSpan? MaxPendingAge { get; set; }

    /// <summary>How many dead messages are tolerated before the check reports Degraded.</summary>
    public long MaxDeadMessages { get; set; }
}
