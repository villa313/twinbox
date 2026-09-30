using System.Diagnostics.Metrics;
using Twinbox.Diagnostics;

namespace Twinbox.Transport;

// Shares the core meter's name, so AddMeter(TwinboxDiagnostics.SourceName) collects these as well.
internal static class InboundDiagnostics
{
    private static readonly Meter Meter = new(TwinboxDiagnostics.SourceName);

    private static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>(
        "twinbox.inbox.dead_lettered", description: "Incoming messages moved to a dead-letter destination.");

    private static readonly Counter<long> Discarded = Meter.CreateCounter<long>(
        "twinbox.inbox.discarded", description: "Incoming messages that failed permanently and were acknowledged without a dead-letter destination.");

    public static void RecordDeadLettered(string transport, string source) =>
        DeadLettered.Add(1, new KeyValuePair<string, object?>("transport", transport), new KeyValuePair<string, object?>("source", source));

    public static void RecordDiscarded(string transport, string source) =>
        Discarded.Add(1, new KeyValuePair<string, object?>("transport", transport), new KeyValuePair<string, object?>("source", source));
}
