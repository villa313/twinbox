using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using Grpc.Core;
using RpcStatus = Google.Rpc.Status;
using Status = Grpc.Core.Status;

namespace Twinbox.GooglePubSub.Tests;

public sealed class ErrorClassificationTests
{
    [Theory]
    [InlineData(StatusCode.NotFound)]
    [InlineData(StatusCode.InvalidArgument)]
    public void RejectionsRetryingCannotFix_ArePermanent(StatusCode code) =>
        Assert.True(GooglePubSubErrors.IsPermanent(Rpc(code)));

    [Theory]
    [InlineData(StatusCode.PermissionDenied)]
    [InlineData(StatusCode.Unauthenticated)]
    [InlineData(StatusCode.ResourceExhausted)]
    [InlineData(StatusCode.Unavailable)]
    [InlineData(StatusCode.DeadlineExceeded)]
    [InlineData(StatusCode.Internal)]
    [InlineData(StatusCode.Aborted)]
    [InlineData(StatusCode.Cancelled)]
    public void AccessThrottlingAndOutages_AreTransient(StatusCode code) =>
        Assert.False(GooglePubSubErrors.IsPermanent(Rpc(code)));

    [Fact]
    public void NonRpcFailures_AreTransient()
    {
        Assert.False(GooglePubSubErrors.IsPermanent(new TimeoutException()));
        Assert.False(GooglePubSubErrors.IsPermanent(new OperationCanceledException()));
        Assert.False(GooglePubSubErrors.IsPermanent(new InvalidOperationException("ordering key paused")));
    }

    [Fact]
    public void QuotaRejectionWithRetryInfo_CarriesTheDelay()
    {
        var error = Rpc(StatusCode.ResourceExhausted, new RetryInfo { RetryDelay = Duration.FromTimeSpan(TimeSpan.FromSeconds(7)) });

        Assert.Equal(TimeSpan.FromSeconds(7), GooglePubSubErrors.RetryAfter(error));
    }

    [Fact]
    public void RetryHints_NeedAQuotaRejectionWithAPositiveDelay()
    {
        Assert.Null(GooglePubSubErrors.RetryAfter(Rpc(StatusCode.ResourceExhausted)));
        Assert.Null(GooglePubSubErrors.RetryAfter(Rpc(StatusCode.ResourceExhausted, new RetryInfo { RetryDelay = new Duration() })));
        Assert.Null(GooglePubSubErrors.RetryAfter(Rpc(StatusCode.Unavailable, new RetryInfo { RetryDelay = Duration.FromTimeSpan(TimeSpan.FromSeconds(7)) })));
        Assert.Null(GooglePubSubErrors.RetryAfter(new TimeoutException()));
    }

    [Fact]
    public void NotFound_IsRecognised()
    {
        Assert.True(GooglePubSubErrors.IsNotFound(Rpc(StatusCode.NotFound)));
        Assert.False(GooglePubSubErrors.IsNotFound(Rpc(StatusCode.PermissionDenied)));
    }

    [Fact]
    public void AlreadyExists_IsRecognised()
    {
        Assert.True(GooglePubSubErrors.IsAlreadyExists(Rpc(StatusCode.AlreadyExists)));
        Assert.False(GooglePubSubErrors.IsAlreadyExists(Rpc(StatusCode.NotFound)));
    }

    private static RpcException Rpc(StatusCode code, IMessage? detail = null)
    {
        var trailers = new Metadata();
        if (detail is not null)
        {
            var status = new RpcStatus { Code = (int)code, Message = code.ToString(), Details = { Any.Pack(detail) } };
            trailers.Add("grpc-status-details-bin", status.ToByteArray());
        }

        return new RpcException(new Status(code, code.ToString()), trailers);
    }
}
