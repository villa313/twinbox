namespace Twinbox;

/// <summary>Buffers messages that are persisted with the current unit of work and delivered after it commits.</summary>
public interface IOutbox
{
    void Send<TMessage>(TMessage message, SendOptions? options = null)
        where TMessage : class;
}
