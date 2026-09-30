using System.Globalization;

namespace Twinbox.Benchmarks.Throughput;

internal readonly record struct Measurement(double Median, double Min, double Max)
{
    /// <summary>One untimed warm-up run, then <paramref name="runs"/> timed ones, each after <paramref name="reset"/>.</summary>
    public static async Task<Measurement> TakeAsync(int runs, Func<Task> reset, Func<Task<double>> run)
    {
        await reset();
        await run();

        var results = new List<double>(runs);
        for (var i = 0; i < runs; i++)
        {
            await reset();
            results.Add(await run());
        }

        results.Sort();
        return new Measurement(results[results.Count / 2], results[0], results[^1]);
    }

    public string Format(string format) =>
        string.Create(CultureInfo.InvariantCulture, $"{Median.ToString(format, CultureInfo.InvariantCulture)} ({Min.ToString(format, CultureInfo.InvariantCulture)}–{Max.ToString(format, CultureInfo.InvariantCulture)})");
}
