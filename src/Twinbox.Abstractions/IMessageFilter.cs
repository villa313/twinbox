namespace Twinbox;

/// <summary>Middleware around every <see cref="IHandle{TMessage}"/> call, inside its inbox transaction; call <c>continuation</c> to carry on.</summary>
public interface IMessageFilter
{
    Task InvokeAsync(object message, MessageContext context, Func<Task> continuation, CancellationToken cancellationToken);
}

/// <summary>Middleware around every <see cref="IHandleBatch{TMessage}"/> call, batches of one included, inside the batch's inbox transaction.</summary>
public interface IBatchMessageFilter
{
    Task InvokeAsync(IReadOnlyList<BatchItem<object>> batch, Func<Task> continuation, CancellationToken cancellationToken);
}

/// <summary>Runs as a message is sent, e.g. to stamp headers from the current request.</summary>
public interface IOutgoingMessageFilter
{
    void OnSending(object message, IDictionary<string, string> headers);
}
