namespace Twinbox;

/// <summary>
/// Handles several messages of one type at once, e.g. for bulk inserts. Transports that receive in batches pass them
/// through; others deliver batches of one. Already-processed messages are filtered out before the call.
/// </summary>
public interface IHandleBatch<TMessage>
    where TMessage : class
{
    Task HandleAsync(IReadOnlyList<BatchItem<TMessage>> batch, CancellationToken cancellationToken);
}

public sealed record BatchItem<TMessage>(TMessage Message, MessageContext Context)
    where TMessage : class;
