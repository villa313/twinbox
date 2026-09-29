namespace Twinbox;

public interface IHandle<in TMessage>
    where TMessage : class
{
    Task HandleAsync(TMessage message, MessageContext context, CancellationToken cancellationToken);
}
