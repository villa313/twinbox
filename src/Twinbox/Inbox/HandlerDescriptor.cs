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

internal abstract class BatchHandlerDescriptor(Type messageType, string consumerName)
    : HandlerDescriptor(messageType, consumerName)
{
    public abstract Task InvokeBatchAsync(
        IServiceProvider services,
        IReadOnlyList<(object Message, MessageContext Context)> items,
        CancellationToken cancellationToken);

    public override Task InvokeAsync(IServiceProvider services, object message, MessageContext context, CancellationToken cancellationToken) =>
        InvokeBatchAsync(services, [(message, context)], cancellationToken);
}

internal sealed class BatchHandlerDescriptor<THandler, TMessage>(string consumerName)
    : BatchHandlerDescriptor(typeof(TMessage), consumerName)
    where THandler : class, IHandleBatch<TMessage>
    where TMessage : class
{
    public override Task InvokeBatchAsync(
        IServiceProvider services,
        IReadOnlyList<(object Message, MessageContext Context)> items,
        CancellationToken cancellationToken) =>
        services.GetRequiredService<THandler>().HandleAsync(
            [.. items.Select(i => new BatchItem<TMessage>((TMessage)i.Message, i.Context))],
            cancellationToken);
}
