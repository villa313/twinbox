using BenchmarkDotNet.Attributes;
using Twinbox.Serialization;
using Twinbox.Transport;

namespace Twinbox.Benchmarks.Micro;

/// <summary>Cost of reading ids from foreign header schemes on receive, and of encoding headers for a relational row.</summary>
[MemoryDiagnoser]
public class HeaderProfileBenchmarks
{
    private readonly HeaderProfiles _none = new([]);
    private readonly HeaderProfiles _cloudEvents = new([HeaderProfile.CloudEvents("urn:shop")]);
    private readonly HeaderProfiles _twoProfiles = new([HeaderProfile.Prefixed("legacy"), HeaderProfile.CloudEvents("urn:shop")]);

    private readonly Dictionary<string, string> _outgoingHeaders = new()
    {
        ["tenant-region"] = "eu-west",
        ["source-system"] = "checkout",
        ["schema-version"] = "3",
    };

    private IncomingMessage _message = null!;

    [GlobalSetup]
    public void Setup() => _message = new IncomingMessage(
        "broker-assigned-id",
        "OrderPlaced",
        "orders",
        Array.Empty<byte>(),
        "application/json",
        new Dictionary<string, string>
        {
            ["ce-id"] = "0199a1b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b",
            ["ce-type"] = "OrderPlaced",
            ["ce-source"] = "urn:shop",
            ["ce-specversion"] = "1.0",
            ["ce-time"] = "2026-09-29T10:00:00.0000000+00:00",
            [TransportHeaders.TraceParent] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        },
        1,
        null);

    [Benchmark(Baseline = true, Description = "Read, no profiles")]
    public IncomingMessage ReadWithoutProfiles() => _none.Read(_message);

    [Benchmark(Description = "Read, CloudEvents profile")]
    public IncomingMessage ReadCloudEvents() => _cloudEvents.Read(_message);

    [Benchmark(Description = "Read, second of two profiles matches")]
    public IncomingMessage ReadSecondProfile() => _twoProfiles.Read(_message);

    [Benchmark(Description = "Write, CloudEvents profile")]
    public Dictionary<string, string> WriteCloudEvents()
    {
        var headers = new Dictionary<string, string>();
        _cloudEvents.Write(headers, "0199a1b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b", "OrderPlaced", null, DateTimeOffset.UnixEpoch);
        return headers;
    }

    [Benchmark(Description = "Encode 3 headers as JSON (relational row)")]
    public string? EncodeHeaders() => HeaderCodec.Encode(_outgoingHeaders);
}
