namespace Twinbox.Transport;

internal static class RetryBackoff
{
    /// <summary>Doubles <paramref name="initial"/> per failed attempt, capped at <paramref name="max"/>.</summary>
    public static TimeSpan Delay(TimeSpan initial, TimeSpan max, int attempt)
    {
        var factor = Math.Pow(2, Math.Clamp(attempt - 1, 0, 30));
        var ticks = Math.Min(initial.Ticks * factor, max.Ticks);
        return TimeSpan.FromTicks((long)ticks);
    }
}
