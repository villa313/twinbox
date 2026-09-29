namespace Twinbox.Storage;

/// <summary>Scoped buffer of messages sent through <see cref="IOutbox"/> that a store persists with its unit of work.</summary>
public interface IOutboxSession
{
    bool HasPending { get; }

    IReadOnlyList<OutboxMessage> TakePending();
}
