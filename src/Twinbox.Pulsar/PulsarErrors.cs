using DotPulsar.Exceptions;

namespace Twinbox.Pulsar;

internal static class PulsarErrors
{
    /// <summary>True for rejections a retry cannot fix, so the outbox dead-letters instead of backing off.</summary>
    public static bool IsPermanent(Exception error) => Cause(error) is
        TopicNotFoundException or
        InvalidTopicNameException or
        AuthorizationException or
        TooLargeMessageException or
        TopicTerminatedException or
        IncompatibleSchemaException or
        NotAllowedException;

    /// <summary>A faulted producer or consumer wraps the error that faulted it.</summary>
    public static Exception Cause(Exception error) => error is FaultedException { InnerException: { } inner } ? inner : error;
}
