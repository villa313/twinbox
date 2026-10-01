namespace Twinbox.Testing;

/// <summary>An <see cref="IOutbox"/> that only records sends, for constructing code that needs one when the test doesn't deliver.</summary>
public sealed class RecordingOutbox : IOutbox
{
    private readonly List<RecordedSend> _sent = [];
    private readonly object _gate = new();

    public IReadOnlyList<RecordedSend> Sent
    {
        get
        {
            lock (_gate)
            {
                return [.. _sent];
            }
        }
    }

    public void Send<TMessage>(TMessage message, SendOptions? options = null)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (_gate)
        {
            _sent.Add(new RecordedSend(message, options));
        }
    }

    public IReadOnlyList<TMessage> Messages<TMessage>()
        where TMessage : class => [.. Sent.Select(send => send.Message).OfType<TMessage>()];
}

public sealed record RecordedSend(object Message, SendOptions? Options);
