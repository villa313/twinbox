using StackExchange.Redis;

namespace Twinbox.RedisStreams;

internal static class RedisStreamsErrors
{
    /// <summary>A key of another type won't change on retry; connection trouble, OOM, failovers and missing ACL grants may,
    /// and an ACL fix should release the backlog rather than find it dead-lettered.</summary>
    public static bool IsPermanent(Exception error) =>
        error is RedisServerException { Message: var message } && message.StartsWith("WRONGTYPE", StringComparison.Ordinal);

    public static bool IsGroupExisting(Exception error) =>
        error is RedisServerException { Message: var message } && message.StartsWith("BUSYGROUP", StringComparison.Ordinal);
}
