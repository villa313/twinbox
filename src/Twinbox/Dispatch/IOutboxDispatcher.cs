namespace Twinbox;

public interface IOutboxDispatcher
{
    /// <summary>Claims and delivers one batch; returns how many messages were claimed.</summary>
    Task<int> DispatchBatchAsync(CancellationToken cancellationToken);
}
