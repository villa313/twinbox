namespace Twinbox;

public static class ReplyExtensions
{
    /// <summary>Sends <paramref name="response"/> to the request's reply address, correlated with the request.</summary>
    public static void Reply<TResponse>(this IOutbox outbox, MessageContext request, TResponse response, string? transport = null)
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(outbox);
        ArgumentNullException.ThrowIfNull(request);
        var options = new SendOptions
        {
            Destination = request.ReplyTo
                ?? throw new InvalidOperationException($"Message {request.MessageId} has no reply address; the sender must set SendOptions.ReplyTo."),
            Transport = transport,
            CorrelationId = request.CorrelationId ?? request.MessageId,
        };

        // A reply address is already physical, so the destination prefix must not be applied again.
        if (outbox is Messaging.OutboxBuffer buffer)
        {
            buffer.Send(response, options, physicalDestination: true);
        }
        else
        {
            outbox.Send(response, options);
        }
    }
}
