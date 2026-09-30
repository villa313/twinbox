using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Twinbox.Nats.Tests;

public sealed class ErrorClassificationTests
{
    [Theory]
    [InlineData(400, NatsErrors.MessageTooLarge)]
    [InlineData(503, NatsErrors.MessageTooLarge)]
    [InlineData(400, 10071)]
    [InlineData(413, 10000)]
    public void RejectionsRetryingCannotFix_ArePermanent(int code, int errCode) =>
        Assert.True(NatsErrors.IsPermanent(new NatsJSApiException(Error(code, errCode))));

    [Theory]
    [InlineData(503, 10077)]
    [InlineData(503, 10039)]
    [InlineData(500, 10049)]
    [InlineData(408, 10000)]
    [InlineData(403, 10000)]
    public void UnavailabilityFullStreamsAndDenials_AreTransient(int code, int errCode) =>
        Assert.False(NatsErrors.IsPermanent(new NatsJSApiException(Error(code, errCode))));

    [Fact]
    public void PermissionViolation_IsTransient() =>
        Assert.False(NatsErrors.IsPermanent(new NatsServerException("Permissions Violation for Publish to \"orders.placed\"")));

    [Fact]
    public void OtherServerErrors_AreTransient()
    {
        Assert.False(NatsErrors.IsPermanent(new NatsServerException("Stale Connection")));
        Assert.False(NatsErrors.IsPermanent(new NatsServerException("Authorization Violation")));
    }

    [Fact]
    public void PayloadOverTheServerMaximum_IsPermanent() =>
        Assert.True(NatsErrors.IsPermanent(new NatsException("Payload size 2097300 exceeds server's maximum payload size 1048576")));

    [Fact]
    public void NoResponseAndOtherFailures_AreNotPermanent()
    {
        Assert.False(NatsErrors.IsPermanent(new NatsJSPublishNoResponseException()));
        Assert.False(NatsErrors.IsPermanent(new NatsNoRespondersException()));
        Assert.False(NatsErrors.IsPermanent(new NatsNoReplyException()));
        Assert.False(NatsErrors.IsPermanent(new TimeoutException()));
        Assert.False(NatsErrors.IsPermanent(new OperationCanceledException()));
    }

    [Fact]
    public void MissingReplies_AreRecognisedAsNoResponse()
    {
        Assert.True(NatsErrors.IsNoResponse(new NatsJSPublishNoResponseException()));
        Assert.True(NatsErrors.IsNoResponse(new NatsNoRespondersException()));
        Assert.False(NatsErrors.IsNoResponse(new NatsNoReplyException()));
        Assert.False(NatsErrors.IsNoResponse(new TimeoutException()));
    }

    private static ApiError Error(int code, int errCode) => new() { Code = code, ErrCode = errCode, Description = "test" };
}
