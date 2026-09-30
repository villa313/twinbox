using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Twinbox.RabbitMQ;

internal static class RabbitMQErrors
{
    /// <summary>
    /// A missing exchange won't appear on retry; unroutable messages may route once a consumer binds. Access refusals stay
    /// transient: a permissions fix should release the backlog rather than find it dead-lettered.
    /// </summary>
    public static bool IsPermanent(Exception error, bool unroutableIsPermanent) => error switch
    {
        PublishException { IsReturn: true } => unroutableIsPermanent,
        OperationInterruptedException { ShutdownReason.ReplyCode: Constants.NotFound } => true,
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
