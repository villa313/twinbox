using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Twinbox.RabbitMQ;

internal static class RabbitMqErrors
{
    /// <summary>Missing or forbidden exchanges won't fix themselves on retry; unroutable messages may, once a consumer binds.</summary>
    public static bool IsPermanent(Exception error, bool unroutableIsPermanent) => error switch
    {
        PublishException { IsReturn: true } => unroutableIsPermanent,
        OperationInterruptedException { ShutdownReason.ReplyCode: Constants.NotFound or Constants.AccessRefused } => true,
        _ => false,
    };

    public static bool IsUnroutable(Exception error) => error is PublishException { IsReturn: true };

    public static string Describe(Exception error, string exchange, string routingKey) => error switch
    {
        PublishReturnException returned =>
            $"Exchange '{exchange}' returned the message as unroutable for routing key '{routingKey}' ({returned.ReplyCode} {returned.ReplyText}).",
        PublishException =>
            $"Exchange '{exchange}' returned the message as unroutable for routing key '{routingKey}'.",
        OperationInterruptedException { ShutdownReason: { } reason } =>
            $"Publishing to exchange '{exchange}' was rejected: {reason.ReplyCode} {reason.ReplyText}.",
        _ => $"Publishing to exchange '{exchange}' failed.",
    };
}
