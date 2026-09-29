namespace Twinbox.Transport;

/// <summary>
/// Entry point transports call for each received message. Returning means the message can be acknowledged;
/// <see cref="PermanentDeliveryException"/> means dead-letter it; any other exception means redeliver.
/// </summary>
public interface IInboundPipeline
{
    Task ProcessAsync(IncomingMessage message, CancellationToken cancellationToken);
}
