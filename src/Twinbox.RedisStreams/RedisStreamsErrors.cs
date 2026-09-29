using StackExchange.Redis;

namespace Twinbox.RedisStreams;

internal static class RedisStreamsErrors
{
    /// <summary>A key of another type or a missing ACL grant won't change on retry; connection trouble, OOM and failovers may.</summary>
    public static bool IsPermanent(Exception error) =>
        error is RedisServerException { Message: var message } &&
        (message.StartsWith("WRONGTYPE", StringComparison.Ordinal) || message.StartsWith("NOPERM", StringComparison.Ordinal));

    public static bool IsGroupExisting(Exception error) =>
        error is RedisServerException { Message: var message } && message.StartsWith("BUSYGROUP", StringComparison.Ordinal);
}
