using System.Net;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Sns = Amazon.SimpleNotificationService.Model;

namespace Twinbox.AmazonSqs.Tests;

public sealed class ErrorClassificationTests
{
    public static TheoryData<Exception> Permanent =>
    [
        new QueueDoesNotExistException("gone"),
        new InvalidMessageContentsException("bad chars"),
        new Sns.NotFoundException("no topic"),
        new Sns.InvalidParameterException("too long"),
        new Sns.InvalidParameterValueException("bad value"),
        Coded(new AmazonSQSException("too large"), "InvalidParameterValue"),
        Coded(new AmazonSQSException("gone"), "AWS.SimpleQueueService.NonExistentQueue"),
    ];

    public static TheoryData<Exception> Transient =>
    [
        new Sns.AuthorizationErrorException("denied"),
        Coded(new AmazonSQSException("denied"), "AccessDenied"),
        Coded(new AmazonSimpleNotificationServiceException("denied"), "AuthorizationError"),
        new RequestThrottledException("slow down"),
        new Sns.ThrottledException("slow down"),
        new Sns.InternalErrorException("oops"),
        new KmsThrottledException("kms"),
        Coded(new AmazonSQSException("unavailable"), "ServiceUnavailable"),
        Coded(new AmazonSQSException("expired"), "ExpiredToken"),
        new AmazonServiceException("timeout") { StatusCode = HttpStatusCode.GatewayTimeout },
        new HttpRequestException("connection reset"),
        new TimeoutException(),
        new OperationCanceledException(),
    ];

    [Theory]
    [MemberData(nameof(Permanent))]
    public void RejectionsRetryingCannotFix_ArePermanent(Exception error) => Assert.True(AmazonSqsErrors.IsPermanent(error));

    [Theory]
    [MemberData(nameof(Transient))]
    public void ThrottlingAndNetworkHiccups_AreTransient(Exception error) => Assert.False(AmazonSqsErrors.IsPermanent(error));

    [Fact]
    public void MissingQueues_AreRecognised()
    {
        Assert.True(AmazonSqsErrors.IsMissingQueue(new QueueDoesNotExistException("gone")));
        Assert.True(AmazonSqsErrors.IsMissingQueue(Coded(new AmazonSQSException("gone"), "AWS.SimpleQueueService.NonExistentQueue")));
        Assert.False(AmazonSqsErrors.IsMissingQueue(new Sns.NotFoundException("no topic")));
    }

    private static T Coded<T>(T error, string code)
        where T : AmazonServiceException
    {
        error.ErrorCode = code;
        return error;
    }
}
