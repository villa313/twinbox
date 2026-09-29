namespace Twinbox;

/// <summary>Middleware around every handler call, inside its inbox transaction; call <c>continuation</c> to carry on.</summary>
public interface IMessageFilter
{
    Task InvokeAsync(object message, MessageContext context, Func<Task> continuation, CancellationToken cancellationToken);
}

/// <summary>Runs as a message is sent, e.g. to stamp headers from the current request.</summary>
public interface IOutgoingMessageFilter
{
    void OnSending(object message, IDictionary<string, string> headers);
}
