namespace Twinbox;

public static class ReplyExtensions
{
    /// <summary>Sends <paramref name="response"/> to the request's reply address, correlated with the request.</summary>
    public static void Reply<TResponse>(this IOutbox outbox, MessageContext request, TResponse response, string? transport = null)
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(outbox);
        ArgumentNullException.ThrowIfNull(request);
        outbox.Send(response, new SendOptions
        {
            Destination = request.ReplyTo
                ?? throw new InvalidOperationException($"Message {request.MessageId} has no reply address; the sender must set SendOptions.ReplyTo."),
            Transport = transport,
            CorrelationId = request.CorrelationId ?? request.MessageId,
        });
    }
}
