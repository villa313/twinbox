using System.Diagnostics.Metrics;
using Twinbox.Diagnostics;

namespace Twinbox.Tests.Shared;

/// <summary>Sums one Twinbox counter, optionally only measurements whose "source" tag matches.</summary>
internal sealed class CounterProbe : IDisposable
{
    private readonly MeterListener _listener = new();
    private long _value;

    public CounterProbe(string instrumentName, string? source = null)
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == TwinboxDiagnostics.SourceName && instrument.Name == instrumentName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            if (source is null || HasSource(tags, source))
            {
                Interlocked.Add(ref _value, measurement);
            }
        });
        _listener.Start();
    }

    public long Value => Interlocked.Read(ref _value);

    public void Dispose() => _listener.Dispose();

    private static bool HasSource(ReadOnlySpan<KeyValuePair<string, object?>> tags, string source)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "source" && Equals(tag.Value, source))
            {
                return true;
            }
        }

        return false;
    }
}
