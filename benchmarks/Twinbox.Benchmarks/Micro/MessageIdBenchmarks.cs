using BenchmarkDotNet.Attributes;
using Twinbox.Storage;

namespace Twinbox.Benchmarks.Micro;

/// <summary>net8.0 builds use Twinbox's own UUIDv7 code; net9.0+ builds use Guid.CreateVersion7. Run with --runtimes net8.0 net10.0.</summary>
[MemoryDiagnoser]
public class MessageIdBenchmarks
{
    private readonly Uuid7MessageIdGenerator _generator = new();
    private readonly DateTimeOffset _now = DateTimeOffset.UtcNow;

    [Benchmark(Baseline = true, Description = "Guid.NewGuid (v4)")]
    public Guid NewGuid() => Guid.NewGuid();

    [Benchmark(Description = "Uuid7MessageIdGenerator")]
    public Guid Uuid7() => _generator.NewId(_now);
}
