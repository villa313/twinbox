namespace Twinbox.Transport;

/// <summary>
/// Entry point transports call for each received message. Returning means the message can be acknowledged;
/// <see cref="PermanentDeliveryException"/> means dead-letter it; any other exception means redeliver.
/// </summary>
public interface IInboundPipeline
{
    Task ProcessAsync(IncomingMessage message, CancellationToken cancellationToken);

    /// <summary>
    /// Processes messages received together, giving batch handlers the whole group. It succeeds or fails as a unit, so a
    /// transport that sees a failure can retry the messages one by one to isolate the bad one.
    /// </summary>
    async Task ProcessBatchAsync(IReadOnlyList<IncomingMessage> messages, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        foreach (var message in messages)
        {
            await ProcessAsync(message, cancellationToken).ConfigureAwait(false);
        }
    }
}
