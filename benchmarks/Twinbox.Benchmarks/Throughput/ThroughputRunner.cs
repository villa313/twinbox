using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;

namespace Twinbox.Benchmarks.Throughput;

/// <summary>Multi-second runs against real databases, which BenchmarkDotNet's per-invocation model fits poorly.</summary>
internal static class ThroughputRunner
{
    private static readonly int[] MessagesPerUnit = [0, 1, 10];
    private static readonly int[] Workers = [1, 8];
    private static readonly int[] BatchSizes = [1, 50, 200];
    private static readonly int[] Dispatchers = [1, 4];

    public static async Task<int> RunAsync(string[] args)
    {
        var runs = int.Parse(Option(args, "--runs") ?? "5", CultureInfo.InvariantCulture);
        var databases = (Option(args, "--databases") ?? "postgres,sqlserver,sqlite").Split(',');
        var scenarios = (Option(args, "--scenarios") ?? "savechanges,dispatch,inbox").Split(',');
        var output = Option(args, "--output");

        var report = new StringBuilder();
        void Write(string line)
        {
            Console.WriteLine(line);
            report.AppendLine(line);
        }

        Write($"{RuntimeInformation.FrameworkDescription}, {RuntimeInformation.OSDescription}, {RuntimeInformation.ProcessArchitecture}, {Environment.ProcessorCount} logical cores; median of {runs} runs (min–max).");
        foreach (var database in databases.Select(Create))
        {
            await using (database)
            {
                await database.StartAsync();
                BenchStore[] stores = database.HasAdoStore
                    ? [new BenchStore(database, ado: false), new BenchStore(database, ado: true)]
                    : [new BenchStore(database, ado: false)];

                if (scenarios.Contains("savechanges"))
                {
                    await RunSaveChangesAsync(stores, runs, Write);
                }

                if (scenarios.Contains("dispatch"))
                {
                    await RunDispatchAsync(stores, runs, Write);
                }

                if (scenarios.Contains("inbox"))
                {
                    await RunInboxAsync(stores, runs, Write);
                }
            }
        }

        if (output is not null)
        {
            await File.WriteAllTextAsync(output, report.ToString());
        }

        return 0;
    }

    private static async Task RunSaveChangesAsync(BenchStore[] stores, int runs, Action<string> write)
    {
        write("");
        write("| Store | Messages per transaction | Workers | Transactions/s | Outbox messages/s | vs. no outbox |");
        write("|---|---:|---:|---:|---:|---:|");
        foreach (var store in stores)
        {
            foreach (var workers in Workers)
            {
                var units = workers == 1 ? 1000 : 4000;
                double? baseline = null;
                foreach (var messages in MessagesPerUnit)
                {
                    var result = await SaveChangesScenario.RunAsync(store, messages, workers, units, runs);
                    baseline ??= result.Median;
                    write($"| {store.Name} | {messages} | {workers} | {result.Format("N0")} | {Number(result.Median * messages, "N0")} | {Number(result.Median / baseline.Value, "P0")} |");
                }
            }
        }
    }

    private static async Task RunDispatchAsync(BenchStore[] stores, int runs, Action<string> write)
    {
        write("");
        write("| Store | Batch size | Dispatchers | Messages/s |");
        write("|---|---:|---:|---:|");
        foreach (var store in stores)
        {
            foreach (var batchSize in BatchSizes)
            {
                foreach (var dispatchers in Dispatchers)
                {
                    var messages = batchSize == 1 ? 2000 : 20000;
                    var result = await DispatchScenario.RunAsync(store, batchSize, dispatchers, messages, runs);
                    write($"| {store.Name} | {batchSize} | {dispatchers} | {result.Format("N0")} |");
                }
            }
        }
    }

    private static async Task RunInboxAsync(BenchStore[] stores, int runs, Action<string> write)
    {
        write("");
        write("| Store | Inbox off (ms/msg) | Inbox on (ms/msg) | Dedup overhead | Duplicate skipped (ms/msg) |");
        write("|---|---:|---:|---:|---:|");
        foreach (var store in stores)
        {
            var result = await InboxScenario.RunAsync(store, 1000, runs);
            var overhead = result.InboxOn.Median - result.InboxOff.Median;
            write($"| {store.Name} | {result.InboxOff.Format("F3")} | {result.InboxOn.Format("F3")} | {Number(overhead, "+0.000;-0.000")} ms | {result.Duplicate.Format("F3")} |");
        }
    }

    private static string Number(double value, string format) => value.ToString(format, CultureInfo.InvariantCulture);

    private static BenchDatabase Create(string name) => name.Trim() switch
    {
        "postgres" => new PostgreSqlBench(),
        "sqlserver" => new SqlServerBench(),
        "sqlite" => new SqliteBench(),
        _ => throw new ArgumentException($"Unknown database '{name}'; use postgres, sqlserver or sqlite."),
    };

    private static string? Option(string[] args, string name)
    {
        var index = Array.IndexOf(args, name);
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }
}
