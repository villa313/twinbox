using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Twinbox.Diagnostics;

public static class TwinboxDiagnostics
{
    /// <summary>Name to pass to AddSource / AddMeter when configuring OpenTelemetry.</summary>
    public const string SourceName = "Twinbox";

    internal static readonly ActivitySource ActivitySource = new(SourceName);

    internal static readonly Meter Meter = new(SourceName);

    internal static readonly Counter<long> MessagesSent =
        Meter.CreateCounter<long>("twinbox.outbox.sent", description: "Outgoing messages delivered to a transport.");

    internal static readonly Counter<long> SendFailures =
        Meter.CreateCounter<long>("twinbox.outbox.failed", description: "Failed delivery attempts.");

    internal static readonly Counter<long> MessagesDeadLettered =
        Meter.CreateCounter<long>("twinbox.outbox.dead_lettered", description: "Outgoing messages that gave up.");

    internal static readonly Histogram<double> DeliveryLatency =
        Meter.CreateHistogram<double>("twinbox.outbox.delivery_latency", unit: "ms", description: "Time from send to delivery.");

    internal static readonly Counter<long> MessagesProcessed =
        Meter.CreateCounter<long>("twinbox.inbox.processed", description: "Incoming messages handled.");

    internal static readonly Counter<long> DuplicatesSkipped =
        Meter.CreateCounter<long>("twinbox.inbox.duplicates", description: "Incoming messages skipped as already processed.");

    internal static Activity? StartActivity(string name, ActivityKind kind, string? traceParent, string? traceState = null)
    {
        ActivityContext.TryParse(traceParent, traceState, out var parent);
        return ActivitySource.StartActivity(name, kind, parent);
    }

    /// <summary>Follows the OpenTelemetry convention by hand, since Activity.AddException needs .NET 9.</summary>
    internal static void RecordError(Activity? activity, Exception error)
    {
        if (activity is null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, error.Message);
        activity.SetTag("error.type", error.GetType().FullName);
        activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
        {
            ["exception.type"] = error.GetType().FullName,
            ["exception.message"] = error.Message,
            ["exception.stacktrace"] = error.ToString(),
        }));
    }
}
