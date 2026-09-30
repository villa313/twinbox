using Azure.Messaging.EventHubs;
using Reason = Azure.Messaging.EventHubs.EventHubsException.FailureReason;

namespace Twinbox.EventHubs.Tests;

public sealed class ErrorClassificationTests
{
    [Theory]
    [InlineData(Reason.ResourceNotFound)]
    [InlineData(Reason.MessageSizeExceeded)]
    public void RejectionsRetryingCannotFix_ArePermanent(Reason reason) =>
        Assert.True(EventHubsErrors.IsPermanent(new EventHubsException(false, "orders", "rejected", reason)));

    [Theory]
    [InlineData(Reason.ServiceBusy)]
    [InlineData(Reason.QuotaExceeded)]
    [InlineData(Reason.ServiceTimeout)]
    [InlineData(Reason.ServiceCommunicationProblem)]
    [InlineData(Reason.GeneralError)]
    [InlineData(Reason.ClientClosed)]
    [InlineData(Reason.ProducerDisconnected)]
    public void ThrottlingAndServiceHiccups_AreTransient(Reason reason) =>
        Assert.False(EventHubsErrors.IsPermanent(new EventHubsException(true, "orders", "try again", reason)));

    [Fact]
    public void AccessDenied_IsTransientSoAnRbacFixReleasesTheBacklog() =>
        Assert.False(EventHubsErrors.IsPermanent(new UnauthorizedAccessException("Unauthorized access. 'Send' claim(s) are required.")));

    [Fact]
    public void OtherFailures_AreTransient()
    {
        Assert.False(EventHubsErrors.IsPermanent(new TimeoutException()));
        Assert.False(EventHubsErrors.IsPermanent(new OperationCanceledException()));
    }
}
