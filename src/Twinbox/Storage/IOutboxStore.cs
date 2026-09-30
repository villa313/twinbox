namespace Twinbox.Storage;

/// <summary>
/// Storage contract for outbox providers. Every implementation must pass the conformance suite in Twinbox.Testing.
/// </summary>
public interface IOutboxStore
{
    /// <summary>Non-transactional append, for stores without an ambient unit of work.</summary>
    Task AppendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken);

    /// <summary>
    /// Leases due messages to <see cref="OutboxClaim.Owner"/>. Only the oldest unsent message of each partition
    /// is eligible, and a message whose lease has expired is eligible again.
    /// </summary>
    Task<IReadOnlyList<OutboxMessage>> ClaimAsync(OutboxClaim claim, CancellationToken cancellationToken);

    /// <summary>Applies outcomes, ignoring any message whose lease is no longer held by <paramref name="owner"/>.</summary>
    Task CompleteAsync(string owner, IReadOnlyList<DispatchOutcome> outcomes, CancellationToken cancellationToken);

    Task<int> PurgeAsync(OutboxPurge purge, CancellationToken cancellationToken);

    Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken);
}

public sealed record OutboxClaim(string Owner, int BatchSize, DateTimeOffset Now, TimeSpan LeaseDuration);

/// <summary>Result of one delivery attempt. <see cref="OutboxMessageStatus.Pending"/> means reschedule at <see cref="AvailableAt"/>.</summary>
public sealed record DispatchOutcome(
    Guid MessageId,
    OutboxMessageStatus Status,
    int Attempts,
    DateTimeOffset? AvailableAt = null,
    DateTimeOffset? SentAt = null,
    string? Error = null);

public sealed record OutboxPurge(DateTimeOffset SentBefore, DateTimeOffset? DeadBefore, int BatchSize);

/// <summary><see cref="OldestPendingAvailableAt"/> is when the longest-waiting unsent message became due, so delayed sends only age once due.</summary>
public sealed record OutboxStatistics(long PendingCount, DateTimeOffset? OldestPendingAvailableAt, long DeadCount);
