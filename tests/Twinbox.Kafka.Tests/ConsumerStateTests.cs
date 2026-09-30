using Confluent.Kafka;

namespace Twinbox.Kafka.Tests;

public sealed class ConsumerStateTests
{
    private static readonly TopicPartition Partition0 = new("orders", 0);
    private static readonly TopicPartition Partition1 = new("orders", 1);

    [Fact]
    public void AttemptOf_CountsRetriesOfTheSameRecordOnly()
    {
        var retries = new PartitionRetries();
        var failed = new TopicPartitionOffset(Partition0, 5);

        Assert.Equal(1, retries.AttemptOf(failed));

        retries.Failed(failed, 1, resumeAt: 100);
        Assert.Equal(2, retries.AttemptOf(failed));
        Assert.Equal(1, retries.AttemptOf(new TopicPartitionOffset(Partition0, 6)));
        Assert.Equal(1, retries.AttemptOf(new TopicPartitionOffset(Partition1, 5)));

        retries.Failed(failed, 2, resumeAt: 200);
        Assert.Equal(3, retries.AttemptOf(failed));

        retries.Succeeded(Partition0);
        Assert.Equal(1, retries.AttemptOf(failed));
    }

    [Fact]
    public void FailedPartition_StaysPausedUntilItsResumeIsDue()
    {
        var retries = new PartitionRetries();
        retries.Failed(new TopicPartitionOffset(Partition0, 5), 1, resumeAt: 1_000);
        retries.Failed(new TopicPartitionOffset(Partition1, 9), 1, resumeAt: 3_000);

        Assert.True(retries.IsPaused(Partition0));
        Assert.Empty(retries.TakeDue(999));
        Assert.Equal([Partition0], retries.TakeDue(1_000));
        Assert.False(retries.IsPaused(Partition0));
        Assert.True(retries.IsPaused(Partition1));
        Assert.Equal(2, retries.AttemptOf(new TopicPartitionOffset(Partition0, 5)));
    }

    [Fact]
    public void UntilNextResume_IsCappedAndNeverNegative()
    {
        var retries = new PartitionRetries();
        var cap = TimeSpan.FromMilliseconds(100);

        Assert.Equal(cap, retries.UntilNextResume(0, cap));

        retries.Failed(new TopicPartitionOffset(Partition0, 5), 1, resumeAt: 1_030);
        Assert.Equal(TimeSpan.FromMilliseconds(30), retries.UntilNextResume(1_000, cap));
        Assert.Equal(cap, retries.UntilNextResume(0, cap));
        Assert.Equal(TimeSpan.Zero, retries.UntilNextResume(2_000, cap));
    }

    [Fact]
    public void Forget_DropsTheRetryAndPauseOfARevokedPartition()
    {
        var retries = new PartitionRetries();
        var failed = new TopicPartitionOffset(Partition0, 5);
        retries.Failed(failed, 3, resumeAt: 1_000);

        Assert.True(retries.Forget(Partition0));
        Assert.False(retries.Forget(Partition0));
        Assert.False(retries.IsPaused(Partition0));
        Assert.Equal(1, retries.AttemptOf(failed));
        Assert.Empty(retries.TakeDue(long.MaxValue));
    }

    [Fact]
    public void Batches_NeverMixPartitionsAndKeepOffsetOrder()
    {
        var batches = new PartitionBatches(maxBatchSize: 10);

        batches.Add(Record(Partition0, 1));
        batches.Add(Record(Partition1, 7));
        batches.Add(Record(Partition0, 2));
        batches.Add(Record(Partition1, 8));
        batches.Add(Record(Partition0, 3));

        var taken = batches.TakeAll();
        Assert.Equal(2, taken.Count);
        Assert.Equal([1L, 2L, 3L], taken[0].Select(r => r.Offset.Value));
        Assert.All(taken[0], r => Assert.Equal(Partition0, r.TopicPartition));
        Assert.Equal([7L, 8L], taken[1].Select(r => r.Offset.Value));
        Assert.True(batches.IsEmpty);
    }

    [Fact]
    public void Add_ReportsWhenAPartitionsBatchIsFull()
    {
        var batches = new PartitionBatches(maxBatchSize: 2);

        Assert.False(batches.Add(Record(Partition0, 1)));
        Assert.False(batches.Add(Record(Partition1, 1)));
        Assert.True(batches.Add(Record(Partition0, 2)));
    }

    [Fact]
    public void BatchOfOne_IsFullAtOnce() => Assert.True(new PartitionBatches(maxBatchSize: 1).Add(Record(Partition0, 1)));

    [Fact]
    public void Drop_DiscardsTheRecordsOfARevokedPartition()
    {
        var batches = new PartitionBatches(maxBatchSize: 10);
        batches.Add(Record(Partition0, 1));
        batches.Add(Record(Partition1, 1));

        batches.Drop(Partition0);

        var batch = Assert.Single(batches.TakeAll());
        Assert.Equal(Partition1, Assert.Single(batch).TopicPartition);
    }

    private static ConsumeResult<string?, byte[]> Record(TopicPartition partition, long offset) => new()
    {
        TopicPartitionOffset = new TopicPartitionOffset(partition, offset),
        Message = new Message<string?, byte[]> { Value = [] },
    };
}
