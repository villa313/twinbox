using RabbitMQ.Client;

namespace Twinbox.RabbitMQ;

internal static class RabbitMQTopology
{
    public const string DeadLetterExchange = "twinbox.dead-letter";

    public static string DeadLetterQueue(string queue) => $"{queue}.dlq";

    public static Dictionary<string, object?> QueueArguments(string queue, int deliveryLimit) => new()
    {
        ["x-queue-type"] = "quorum",
        ["x-delivery-limit"] = deliveryLimit,
        ["x-dead-letter-exchange"] = DeadLetterExchange,
        ["x-dead-letter-routing-key"] = DeadLetterQueue(queue),
    };

    public static Task DeclareExchangeAsync(IChannel channel, string exchange, CancellationToken cancellationToken)
    {
        // The broker refuses declarations of its built-in "amq." exchanges, which always exist anyway.
        if (exchange.StartsWith("amq.", StringComparison.Ordinal))
        {
            return Task.CompletedTask;
        }

        return channel.ExchangeDeclareAsync(exchange, ExchangeType.Topic, durable: true, autoDelete: false, cancellationToken: cancellationToken);
    }

    public static async Task DeclareListenerAsync(IChannel channel, RabbitMQListener listener, int deliveryLimit, CancellationToken cancellationToken)
    {
        var deadLetterQueue = DeadLetterQueue(listener.Queue);
        await channel.ExchangeDeclareAsync(DeadLetterExchange, ExchangeType.Direct, durable: true, autoDelete: false, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await channel.QueueDeclareAsync(
            deadLetterQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
            cancellationToken: cancellationToken).ConfigureAwait(false);
        await channel.QueueBindAsync(deadLetterQueue, DeadLetterExchange, deadLetterQueue, cancellationToken: cancellationToken).ConfigureAwait(false);

        await DeclareExchangeAsync(channel, listener.Exchange, cancellationToken).ConfigureAwait(false);
        await channel.QueueDeclareAsync(
            listener.Queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: QueueArguments(listener.Queue, deliveryLimit),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        await channel.QueueBindAsync(listener.Queue, listener.Exchange, listener.BindingKey, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
