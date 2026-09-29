using Confluent.Kafka;

namespace Twinbox.Kafka;

internal static class KafkaErrors
{
    /// <summary>True for rejections a retry cannot fix, so the outbox dead-letters instead of backing off.</summary>
    public static bool IsPermanent(Exception error) => error is KafkaException { Error.Code: var code } && IsPermanent(code);

    public static bool IsPermanent(ErrorCode code) => code is
        ErrorCode.UnknownTopicOrPart or
        ErrorCode.Local_UnknownTopic or
        ErrorCode.TopicException or
        ErrorCode.TopicAuthorizationFailed or
        ErrorCode.MsgSizeTooLarge or
        ErrorCode.InvalidConfig or
        ErrorCode.Local_InvalidArg;

    /// <summary>A fatal error leaves an idempotent producer unusable; it has to be replaced.</summary>
    public static bool IsFatal(Exception error) => error is KafkaException { Error.IsFatal: true };
}
