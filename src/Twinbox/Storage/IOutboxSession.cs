namespace Twinbox.Storage;

/// <summary>Scoped buffer of messages sent through <see cref="IOutbox"/> that a store persists with its unit of work.</summary>
public interface IOutboxSession
{
    /// <summary>The scope the messages were sent from.</summary>
    IServiceProvider Services { get; }

    bool HasPending { get; }

    IReadOnlyList<OutboxMessage> TakePending();
}
