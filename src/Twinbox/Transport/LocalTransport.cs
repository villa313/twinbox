namespace Twinbox.Transport;

/// <summary>
/// Delivers straight to this app's own handlers, so the outbox gives durable in-process events without a broker:
/// a handler failure fails the send and the message is retried from the outbox.
/// </summary>
internal sealed class LocalTransport(IInboundPipeline pipeline) : ITransport
{
    public const string TransportName = "local";

    public string Name => TransportName;

    public Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        message.Headers.TryGetValue(TransportHeaders.DeliveryAttempt, out var attempt);
        return pipeline.ProcessAsync(
            new IncomingMessage(
                message.MessageId,
                message.MessageName,
                message.Destination,
                message.Body,
                message.ContentType,
                message.Headers,
                int.TryParse(attempt, out var parsed) ? parsed : 1,
                message.PartitionKey),
            cancellationToken);
    }
}
