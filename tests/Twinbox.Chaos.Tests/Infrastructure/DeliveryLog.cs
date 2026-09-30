using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using Twinbox.Transport;

namespace Twinbox.Chaos.Tests;

/// <summary>The test's message: <see cref="Index"/> counts up within <see cref="Stream"/>, the partition key when set.</summary>
[MessageName("numbered")]
public sealed record Numbered(string? Stream, int Index, int Id);

public sealed record Delivery(Guid MessageId, Numbered Message, int Attempt, string Instance, DateTimeOffset At);

/// <summary>Every transport call and every successful delivery, across all hosts of a test, in completion order.</summary>
public sealed class DeliveryLog
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly ConcurrentQueue<Delivery> _deliveries = new();
    private readonly ConcurrentDictionary<Guid, int> _calls = new();

    public IReadOnlyList<Delivery> Deliveries => [.. _deliveries];

    public int DistinctDelivered => _deliveries.Select(d => d.MessageId).Distinct().Count();

    public int TotalCalls => _calls.Values.Sum();

    public int CallsFor(Guid messageId) => _calls.GetValueOrDefault(messageId);

    public static Numbered Read(TransportMessage message) => Read(message.Body.Span);

    public static Numbered Read(ReadOnlySpan<byte> body) => JsonSerializer.Deserialize<Numbered>(body, Json)!;

    public static int AttemptOf(TransportMessage message) =>
        int.Parse(message.Headers[TransportHeaders.DeliveryAttempt], CultureInfo.InvariantCulture);

    public void RecordCall(TransportMessage message) =>
        _calls.AddOrUpdate(Guid.Parse(message.MessageId), 1, (_, calls) => calls + 1);

    public void RecordDelivery(TransportMessage message, string instance) =>
        _deliveries.Enqueue(new Delivery(Guid.Parse(message.MessageId), Read(message), AttemptOf(message), instance, DateTimeOffset.UtcNow));

    /// <summary>Asserts that, within each stream, messages were first delivered in the order they were sent.</summary>
    public void AssertStreamOrder()
    {
        var firstDeliveries = Deliveries
            .Where(d => d.Message.Stream is not null)
            .DistinctBy(d => d.MessageId);
        foreach (var stream in firstDeliveries.GroupBy(d => d.Message.Stream))
        {
            var indexes = stream.Select(d => d.Message.Index).ToArray();
            Assert.True(
                indexes.SequenceEqual(indexes.Order()),
                $"Stream {stream.Key} was delivered out of order: {string.Join(", ", indexes)}");
        }
    }
}
