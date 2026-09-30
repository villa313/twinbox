using System.Diagnostics;

namespace Twinbox.Chaos.Tests;

internal static class Eventually
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    public static async Task HoldsAsync(Func<Task<bool>> condition, TimeSpan timeout, Func<string> describe)
    {
        var clock = Stopwatch.StartNew();
        while (!await condition())
        {
            if (clock.Elapsed > timeout)
            {
                Assert.Fail($"Timed out after {timeout}: {describe()}");
            }

            await Task.Delay(PollInterval);
        }
    }

    public static Task HoldsAsync(Func<bool> condition, TimeSpan timeout, Func<string> describe) =>
        HoldsAsync(() => Task.FromResult(condition()), timeout, describe);
}
