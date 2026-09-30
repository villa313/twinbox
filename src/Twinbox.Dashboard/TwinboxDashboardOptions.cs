namespace Twinbox.Dashboard;

public sealed class TwinboxDashboardOptions
{
    /// <summary>Serves the dashboard to anyone when no authorization policy is attached. Meant for local development only.</summary>
    public bool AllowAnonymous { get; set; }

    /// <summary>Hides replay and delete, and refuses them on the API.</summary>
    public bool ReadOnly { get; set; }

    /// <summary>Shows payloads in message details as UTF-8 text, capped at 64 KB. Off by default: payloads often hold personal data.</summary>
    public bool ShowPayloads { get; set; }
}
