using Twinbox.Storage;

namespace Twinbox;

/// <summary>Notified when an outgoing message exhausts its retries or fails permanently.</summary>
public interface IDeadLetterObserver
{
    Task OnDeadLetteredAsync(OutboxMessage message, Exception exception, CancellationToken cancellationToken);
}
