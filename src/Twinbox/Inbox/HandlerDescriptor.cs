using Microsoft.Extensions.DependencyInjection;

namespace Twinbox.Inbox;

internal abstract class HandlerDescriptor(Type messageType, string consumerName)
{
    public Type MessageType { get; } = messageType;

    /// <summary>Inbox key alongside the message id, so each handler of a message is deduplicated on its own.</summary>
    public string ConsumerName { get; } = consumerName;

    public abstract Task InvokeAsync(IServiceProvider services, object message, MessageContext context, CancellationToken cancellationToken);
}

internal sealed class HandlerDescriptor<THandler, TMessage>(string consumerName)
    : HandlerDescriptor(typeof(TMessage), consumerName)
    where THandler : class, IHandle<TMessage>
    where TMessage : class
{
    public override Task InvokeAsync(IServiceProvider services, object message, MessageContext context, CancellationToken cancellationToken) =>
        services.GetRequiredService<THandler>().HandleAsync((TMessage)message, context, cancellationToken);
}
