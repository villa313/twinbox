namespace Twinbox.Dispatch;

internal static class RetrySchedule
{
    /// <summary>Exponential backoff with equal jitter, so a burst of failures doesn't retry in lockstep.</summary>
    public static TimeSpan GetDelay(RetryOptions options, int attempt, Random random)
    {
        var exponent = Math.Min(attempt - 1, 30);
        var ceilingTicks = Math.Min(options.MaxDelay.Ticks, options.InitialDelay.Ticks * Math.Pow(2, exponent));
        var half = ceilingTicks / 2;
        return TimeSpan.FromTicks((long)(half + (random.NextDouble() * half)));
    }
}
