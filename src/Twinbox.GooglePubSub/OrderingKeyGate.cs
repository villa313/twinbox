namespace Twinbox.GooglePubSub;

/// <summary>Holds back messages of an ordering key while an earlier one awaits redelivery, which Pub/Sub may interleave with later ones.</summary>
internal sealed class OrderingKeyGate(TimeSpan staleAfter, TimeProvider timeProvider)
{
    private readonly Dictionary<string, Dictionary<string, Pending>> _pending = new(StringComparer.Ordinal);
    private long _sequence;

    /// <summary>False when an earlier message of the key is still outstanding, so this one must be nacked and wait its turn.</summary>
    public bool TryEnter(string orderingKey, string messageId, DateTimeOffset publishTime)
    {
        lock (_pending)
        {
            if (!_pending.TryGetValue(orderingKey, out var pending))
            {
                return true;
            }

            // An earlier message that never returns, e.g. forwarded to a dead-letter topic, must not stall the key forever.
            var now = timeProvider.GetTimestamp();
            foreach (var id in pending.Where(p => timeProvider.GetElapsedTime(p.Value.SeenAt, now) > staleAfter).Select(p => p.Key).ToList())
            {
                pending.Remove(id);
            }

            var self = pending.GetValueOrDefault(messageId) ?? new Pending(publishTime, long.MaxValue, now);
            if (pending.Any(p => p.Key != messageId && p.Value.IsBefore(self)))
            {
                pending[messageId] = self with { Sequence = Math.Min(self.Sequence, ++_sequence), SeenAt = now };
                return false;
            }

            pending.Remove(messageId);
            if (pending.Count == 0)
            {
                _pending.Remove(orderingKey);
            }

            return true;
        }
    }

    /// <summary>Records a nacked message, which Pub/Sub will redeliver.</summary>
    public void Fail(string orderingKey, string messageId, DateTimeOffset publishTime)
    {
        lock (_pending)
        {
            if (!_pending.TryGetValue(orderingKey, out var pending))
            {
                pending = new Dictionary<string, Pending>(StringComparer.Ordinal);
                _pending[orderingKey] = pending;
            }

            var sequence = pending.GetValueOrDefault(messageId)?.Sequence ?? ++_sequence;
            pending[messageId] = new Pending(publishTime, sequence, timeProvider.GetTimestamp());
        }
    }

    /// <summary>Messages published in one batch share a publish time; the order they first arrived in breaks the tie.</summary>
    private sealed record Pending(DateTimeOffset PublishTime, long Sequence, long SeenAt)
    {
        public bool IsBefore(Pending other) =>
            PublishTime < other.PublishTime || (PublishTime == other.PublishTime && Sequence < other.Sequence);
    }
}
