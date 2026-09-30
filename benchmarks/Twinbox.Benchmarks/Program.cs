using BenchmarkDotNet.Running;
using Twinbox.Benchmarks.Throughput;

if (args.Contains("--throughput"))
{
    return await ThroughputRunner.RunAsync(args);
}

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return 0;
