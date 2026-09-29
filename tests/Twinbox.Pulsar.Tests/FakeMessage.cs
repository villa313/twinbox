using System.Buffers;
using System.Text;
using DotPulsar;
using DotPulsar.Abstractions;

namespace Twinbox.Pulsar.Tests;

/// <summary>A received message as the consumer would hand it over, without a broker.</summary>
internal sealed class FakeMessage : IMessage
{
    public MessageId MessageId { get; init; } = new(7, 42, -1, -1);

    public ReadOnlySequence<byte> Data { get; init; } = new(Encoding.UTF8.GetBytes("{}"));

    public string ProducerName => "producer";

    public byte[]? SchemaVersion => null;

    public ulong SequenceId => 0;

    public uint RedeliveryCount { get; init; }

    public bool HasEventTime => false;

    public ulong EventTime => 0;

    public DateTime EventTimeAsDateTime => default;

    public DateTimeOffset EventTimeAsDateTimeOffset => default;

    public bool HasBase64EncodedKey { get; init; }

    public bool HasKey => Key is not null;

    public string? Key { get; init; }

    public byte[]? KeyBytes { get; init; }

    public bool HasOrderingKey => false;

    public byte[]? OrderingKey => null;

    public ulong PublishTime => 0;

    public DateTime PublishTimeAsDateTime => default;

    public DateTimeOffset PublishTimeAsDateTimeOffset => default;

    public IReadOnlyDictionary<string, string> Properties { get; init; } = new Dictionary<string, string>();
}
