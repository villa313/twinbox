using Twinbox.Storage;

namespace Twinbox.InMemory;

public sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly object _gate = new();
    private readonly Dictionary<Guid, (long Sequence, OutboxMessage Message)> _rows = [];
    private long _sequence;

    public IReadOnlyList<OutboxMessage> Snapshot()
    {
        lock (_gate)
        {
            return [.. _rows.Values.OrderBy(r => r.Sequence).Select(r => r.Message)];
        }
    }

    public Task AppendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        lock (_gate)
        {
            foreach (var message in messages)
            {
                _rows.Add(message.Id, (++_sequence, message));
            }
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<OutboxMessage>> ClaimAsync(OutboxClaim claim, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(claim);
        lock (_gate)
        {
            var unsent = _rows.Values
                .Where(r => r.Message.Status is OutboxMessageStatus.Pending or OutboxMessageStatus.Processing)
                .OrderBy(r => r.Sequence)
                .ToArray();

            var partitionHeads = unsent
                .Where(r => r.Message.PartitionKey is not null)
                .GroupBy(r => r.Message.PartitionKey!)
                .ToDictionary(g => g.Key, g => g.First().Message.Id);

            var claimed = unsent
                .Where(r => IsDue(r.Message, claim.Now))
                .Where(r => r.Message.PartitionKey is null || partitionHeads[r.Message.PartitionKey] == r.Message.Id)
                .Take(claim.BatchSize)
                .Select(r => r.Message with
                {
                    Status = OutboxMessageStatus.Processing,
                    LeaseOwner = claim.Owner,
                    LeaseUntil = claim.Now + claim.LeaseDuration,
                })
                .ToArray();

            foreach (var message in claimed)
            {
                _rows[message.Id] = (_rows[message.Id].Sequence, message);
            }

            return Task.FromResult<IReadOnlyList<OutboxMessage>>(claimed);
        }
    }

    public Task CompleteAsync(string owner, IReadOnlyList<DispatchOutcome> outcomes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(outcomes);
        lock (_gate)
        {
            foreach (var outcome in outcomes)
            {
                if (!_rows.TryGetValue(outcome.MessageId, out var row)
                    || row.Message.Status != OutboxMessageStatus.Processing
                    || row.Message.LeaseOwner != owner)
                {
                    continue;
                }

                _rows[outcome.MessageId] = (row.Sequence, Apply(row.Message, outcome));
            }
        }

        return Task.CompletedTask;
    }

    public Task<int> PurgeAsync(OutboxPurge purge, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(purge);
        lock (_gate)
        {
            var expired = _rows.Values
                .Select(r => r.Message)
                .Where(m => (m.Status == OutboxMessageStatus.Sent && m.SentAt < purge.SentBefore)
                    || (m.Status == OutboxMessageStatus.Dead && purge.DeadBefore is { } deadBefore && m.CreatedAt < deadBefore))
                .Take(purge.BatchSize)
                .Select(m => m.Id)
                .ToArray();

            foreach (var id in expired)
            {
                _rows.Remove(id);
            }

            return Task.FromResult(expired.Length);
        }
    }

    public Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var messages = _rows.Values.Select(r => r.Message).ToArray();
            var unsent = messages.Where(m => m.Status is OutboxMessageStatus.Pending or OutboxMessageStatus.Processing).ToArray();
            return Task.FromResult(new OutboxStatistics(
                unsent.Length,
                unsent.Length == 0 ? null : unsent.Min(m => m.CreatedAt),
                messages.Count(m => m.Status == OutboxMessageStatus.Dead)));
        }
    }

    private static bool IsDue(OutboxMessage message, DateTimeOffset now) => message.Status switch
    {
        OutboxMessageStatus.Pending => message.AvailableAt <= now,
        OutboxMessageStatus.Processing => message.LeaseUntil < now,
        _ => false,
    };

    private static OutboxMessage Apply(OutboxMessage message, DispatchOutcome outcome) => message with
    {
        Status = outcome.Status,
        Attempts = outcome.Attempts,
        AvailableAt = outcome.AvailableAt ?? message.AvailableAt,
        SentAt = outcome.SentAt,
        LastError = outcome.Error ?? message.LastError,
        LeaseOwner = null,
        LeaseUntil = null,
    };
}
