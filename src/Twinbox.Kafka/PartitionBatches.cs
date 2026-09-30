using Confluent.Kafka;

namespace Twinbox.Kafka;

/// <summary>Groups consumed records by partition, in arrival order, so a batch never spans partitions.</summary>
internal sealed class PartitionBatches(int maxBatchSize)
{
    private readonly List<List<ConsumeResult<string?, byte[]>>> _batches = [];

    public bool IsEmpty => _batches.Count == 0;

    /// <summary>Adds the record to its partition's batch; true once that batch holds the maximum.</summary>
    public bool Add(ConsumeResult<string?, byte[]> record)
    {
        var batch = Find(record.TopicPartition);
        if (batch is null)
        {
            batch = [];
            _batches.Add(batch);
        }

        batch.Add(record);
        return batch.Count >= maxBatchSize;
    }

    public void Drop(TopicPartition partition) => _batches.RemoveAll(b => b[0].TopicPartition == partition);

    public List<List<ConsumeResult<string?, byte[]>>> TakeAll()
    {
        var batches = _batches.ToList();
        _batches.Clear();
        return batches;
    }

    private List<ConsumeResult<string?, byte[]>>? Find(TopicPartition partition) =>
        _batches.Find(b => b[0].TopicPartition == partition);
}
