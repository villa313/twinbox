using Confluent.Kafka;

namespace Twinbox.Kafka;

/// <summary>Tracks, per partition, the record being retried and when its paused partition may resume.</summary>
internal sealed class PartitionRetries
{
    private readonly Dictionary<TopicPartition, RecordAttempt> _attempts = [];
    private readonly Dictionary<TopicPartition, long> _resumeAt = [];

    public bool IsPaused(TopicPartition partition) => _resumeAt.ContainsKey(partition);

    /// <summary>1 for a record seen for the first time; one more than last time for the record being retried.</summary>
    public int AttemptOf(TopicPartitionOffset record) =>
        _attempts.TryGetValue(record.TopicPartition, out var previous) && previous.Offset == record.Offset.Value
            ? previous.Attempt + 1
            : 1;

    /// <summary>Remembers the failed attempt and keeps the partition paused until <paramref name="resumeAt"/> (milliseconds).</summary>
    public void Failed(TopicPartitionOffset record, int attempt, long resumeAt)
    {
        _attempts[record.TopicPartition] = new RecordAttempt(record.Offset.Value, attempt);
        _resumeAt[record.TopicPartition] = resumeAt;
    }

    public void Succeeded(TopicPartition partition) => _attempts.Remove(partition);

    /// <summary>Removes and returns the paused partitions whose retry is due at <paramref name="now"/>.</summary>
    public List<TopicPartition> TakeDue(long now)
    {
        var due = _resumeAt.Where(p => p.Value <= now).Select(p => p.Key).ToList();
        foreach (var partition in due)
        {
            _resumeAt.Remove(partition);
        }

        return due;
    }

    /// <summary>How long a poll may block before the next paused partition is due, at most <paramref name="max"/>.</summary>
    public TimeSpan UntilNextResume(long now, TimeSpan max)
    {
        if (_resumeAt.Count == 0)
        {
            return max;
        }

        var wait = TimeSpan.FromMilliseconds(Math.Max(0, _resumeAt.Values.Min() - now));
        return wait < max ? wait : max;
    }

    /// <summary>Drops everything known about the partition; true when it was paused.</summary>
    public bool Forget(TopicPartition partition)
    {
        _attempts.Remove(partition);
        return _resumeAt.Remove(partition);
    }

    private readonly record struct RecordAttempt(long Offset, int Attempt);
}
