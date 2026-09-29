using Amazon.Runtime;
using Amazon.SQS.Model;
using Sns = Amazon.SimpleNotificationService.Model;

namespace Twinbox.AmazonSqs;

internal static class AmazonSqsErrors
{
    private static readonly HashSet<string> PermanentCodes = new(StringComparer.Ordinal)
    {
        "AWS.SimpleQueueService.NonExistentQueue",
        "QueueDoesNotExist",
        "NotFound",
        "NotFoundException",
        "InvalidParameterValue",
        "InvalidParameterValueException",
        "InvalidParameter",
        "InvalidParameterException",
        "InvalidMessageContents",
        "MessageTooLong",
        "AuthorizationError",
        "AuthorizationErrorException",
        "AccessDenied",
        "AccessDeniedException",
    };

    /// <summary>True for rejections a retry cannot fix, so the outbox dead-letters instead of backing off.</summary>
    public static bool IsPermanent(Exception error) => error switch
    {
        QueueDoesNotExistException or InvalidMessageContentsException => true,
        Sns.NotFoundException or Sns.AuthorizationErrorException or Sns.InvalidParameterException or Sns.InvalidParameterValueException => true,
        AmazonServiceException { ErrorCode: { } code } => PermanentCodes.Contains(code),
        _ => false,
    };

    public static bool IsMissingQueue(Exception error) =>
        error is QueueDoesNotExistException or AmazonServiceException { ErrorCode: "AWS.SimpleQueueService.NonExistentQueue" or "QueueDoesNotExist" };
}
