using Google.Api.Gax.Grpc;
using Google.Rpc;
using Grpc.Core;

namespace Twinbox.GooglePubSub;

internal static class GooglePubSubErrors
{
    /// <summary>Rejections a retry cannot fix. Permission errors stay transient: an IAM fix should release the backlog, not find it dead-lettered.</summary>
    public static bool IsPermanent(Exception error) => error is RpcException { StatusCode: StatusCode.NotFound or StatusCode.InvalidArgument };

    /// <summary>The server's retry hint for a quota rejection; null when there is none.</summary>
    public static TimeSpan? RetryAfter(Exception error)
    {
        if (error is not RpcException { StatusCode: StatusCode.ResourceExhausted } rpc)
        {
            return null;
        }

        var delay = rpc.GetStatusDetail<RetryInfo>()?.RetryDelay?.ToTimeSpan();
        return delay > TimeSpan.Zero ? delay : null;
    }

    public static bool IsNotFound(Exception error) => error is RpcException { StatusCode: StatusCode.NotFound };

    public static bool IsAlreadyExists(Exception error) => error is RpcException { StatusCode: StatusCode.AlreadyExists };
}
