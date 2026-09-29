using System.Buffers;
using System.Text;
using DotPulsar;
using Twinbox.Transport;

namespace Twinbox.Pulsar.Tests;

public sealed class MappingTests
{
    [Fact]
    public void ToMetadata_WritesIdentityContentTypeAndKey()
    {
        var metadata = PulsarMapping.ToMetadata(Outgoing(partitionKey: "customer-7"));

        Assert.Equal("msg-1", metadata[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", metadata[TransportHeaders.MessageName]);
        Assert.Equal("application/json", metadata[PulsarMapping.ContentTypeProperty]);
        Assert.Equal("customer-7", metadata.Key);
    }

    [Fact]
    public void ToMetadata_CopiesHeadersAsProperties()
    {
        var metadata = PulsarMapping.ToMetadata(Outgoing(partitionKey: null));

        Assert.Equal("00-abc-def-01", metadata[TransportHeaders.TraceParent]);
        Assert.Equal("tenant-ü", metadata[TransportHeaders.TenantId]);
        Assert.True(string.IsNullOrEmpty(metadata.Key));
    }

    [Fact]
    public void ToMetadata_LeavesTheSequenceIdToTheProducer() =>
        Assert.Equal(0ul, PulsarMapping.ToMetadata(Outgoing(partitionKey: "customer-7")).SequenceId);

    [Fact]
    public void ToMetadata_IdentityWinsOverStaleHeaders()
    {
        var outgoing = Outgoing(partitionKey: null) with
        {
            Headers = new Dictionary<string, string>
            {
                [TransportHeaders.MessageId] = "stale",
                [TransportHeaders.MessageName] = "stale",
                [PulsarMapping.ContentTypeProperty] = "text/stale",
            },
        };

        var metadata = PulsarMapping.ToMetadata(outgoing);

        Assert.Equal("msg-1", metadata[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", metadata[TransportHeaders.MessageName]);
        Assert.Equal("application/json", metadata[PulsarMapping.ContentTypeProperty]);
    }

    [Fact]
    public void ToIncomingMessage_ReadsIdentityHeadersBodyAndKey()
    {
        var message = new FakeMessage
        {
            Key = "customer-7",
            RedeliveryCount = 2,
            Data = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("""{"orderId":1}""")),
            Properties = new Dictionary<string, string>
            {
                [TransportHeaders.MessageId] = "msg-1",
                [TransportHeaders.MessageName] = "order-placed",
                [PulsarMapping.ContentTypeProperty] = "application/json",
                [TransportHeaders.TraceParent] = "00-abc-def-01",
            },
        };

        var incoming = PulsarMapping.ToIncomingMessage("orders", message, PulsarMapping.DeliveryAttempt(message));

        Assert.Equal("msg-1", incoming.MessageId);
        Assert.Equal("order-placed", incoming.MessageName);
        Assert.Equal("orders", incoming.Source);
        Assert.Equal("application/json", incoming.ContentType);
        Assert.Equal("customer-7", incoming.PartitionKey);
        Assert.Equal("""{"orderId":1}""", Encoding.UTF8.GetString(incoming.Body.Span));
        Assert.Equal("00-abc-def-01", incoming.Headers[TransportHeaders.TraceParent]);
        Assert.Equal(3, incoming.DeliveryAttempt);
    }

    [Fact]
    public void ToIncomingMessage_ForeignMessage_FallsBackToStableCoordinates()
    {
        var message = new FakeMessage { MessageId = new MessageId(12, 3, 1, -1) };

        var first = PulsarMapping.ToIncomingMessage("orders", message);
        var redelivered = PulsarMapping.ToIncomingMessage("orders", new FakeMessage { MessageId = new MessageId(12, 3, 1, -1), RedeliveryCount = 1 });

        Assert.Equal("orders:12:3:1:-1", first.MessageId);
        Assert.Equal(first.MessageId, redelivered.MessageId);
        Assert.Equal(string.Empty, first.MessageName);
        Assert.Equal("application/octet-stream", first.ContentType);
        Assert.Empty(first.Headers);
        Assert.Null(first.PartitionKey);
    }

    [Fact]
    public void ToIncomingMessage_EmptyIdProperty_FallsBackToCoordinates()
    {
        var message = new FakeMessage { Properties = new Dictionary<string, string> { [TransportHeaders.MessageId] = string.Empty } };

        Assert.Equal("orders:7:42:-1:-1", PulsarMapping.ToIncomingMessage("orders", message).MessageId);
    }

    [Theory]
    [InlineData(0u, null, 1)]
    [InlineData(3u, null, 4)]
    [InlineData(0u, 1, 2)]
    [InlineData(0u, 4, 5)]
    [InlineData(5u, 2, 6)]
    public void DeliveryAttempt_IsTheBrokersCountRaisedByLocalFailures(uint redeliveryCount, int? failedAttempt, int expected) =>
        Assert.Equal(expected, PulsarMapping.DeliveryAttempt(new FakeMessage { RedeliveryCount = redeliveryCount }, failedAttempt));

    [Fact]
    public void DeliveryAttempt_DoesNotOverflow() =>
        Assert.Equal(int.MaxValue, PulsarMapping.DeliveryAttempt(new FakeMessage { RedeliveryCount = uint.MaxValue }));

    [Fact]
    public void ToDeadLetter_KeepsTheMessageAndAddsFailureDetails()
    {
        var message = new FakeMessage
        {
            Key = "customer-7",
            Properties = new Dictionary<string, string>
            {
                [TransportHeaders.MessageId] = "msg-1",
                [TransportHeaders.MessageName] = "order-placed",
                [TransportHeaders.TraceParent] = "00-abc-def-01",
            },
        };

        var metadata = PulsarMapping.ToDeadLetter("orders", "billing", message, "PermanentDeliveryException: bad payload");

        Assert.Equal("msg-1", metadata[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", metadata[TransportHeaders.MessageName]);
        Assert.Equal("00-abc-def-01", metadata[TransportHeaders.TraceParent]);
        Assert.Equal("PermanentDeliveryException: bad payload", metadata[PulsarMapping.ErrorProperty]);
        Assert.Equal("orders:7:42:-1:-1", metadata[PulsarMapping.OriginProperty]);
        Assert.Equal("billing", metadata[PulsarMapping.SubscriptionProperty]);
        Assert.Equal("customer-7", metadata.Key);
    }

    [Fact]
    public void ToDeadLetter_PinsTheFallbackIdAndReplacesEarlierFailureDetails()
    {
        var message = new FakeMessage
        {
            Properties = new Dictionary<string, string>
            {
                [PulsarMapping.ErrorProperty] = "old",
                [PulsarMapping.OriginProperty] = "elsewhere:1:1:-1:-1",
                [PulsarMapping.SubscriptionProperty] = "other",
            },
        };

        var metadata = PulsarMapping.ToDeadLetter("orders", "billing", message, "new");

        Assert.Equal("orders:7:42:-1:-1", metadata[TransportHeaders.MessageId]);
        Assert.Equal("new", metadata[PulsarMapping.ErrorProperty]);
        Assert.Equal("orders:7:42:-1:-1", metadata[PulsarMapping.OriginProperty]);
        Assert.Equal("billing", metadata[PulsarMapping.SubscriptionProperty]);
        Assert.True(string.IsNullOrEmpty(metadata.Key));
    }

    [Fact]
    public void ToDeadLetter_KeepsBinaryKeys()
    {
        var message = new FakeMessage { HasBase64EncodedKey = true, Key = "AQID", KeyBytes = [1, 2, 3] };

        Assert.Equal([1, 2, 3], PulsarMapping.ToDeadLetter("orders", "billing", message, "error").KeyBytes);
    }

    [Fact]
    public void ToDeadLetter_TruncatesLongErrors() =>
        Assert.Equal(2000, PulsarMapping.ToDeadLetter("orders", "billing", new FakeMessage(), new string('x', 5000))[PulsarMapping.ErrorProperty]!.Length);

    [Fact]
    public void DeadLetterTopic_AppendsTheSuffix() => Assert.Equal("orders-dlq", PulsarMapping.DeadLetterTopic("orders", "-dlq"));

    private static TransportMessage Outgoing(string? partitionKey) => new(
        "msg-1",
        "order-placed",
        "orders",
        Encoding.UTF8.GetBytes("{}"),
        "application/json",
        new Dictionary<string, string>
        {
            [TransportHeaders.TraceParent] = "00-abc-def-01",
            [TransportHeaders.TenantId] = "tenant-ü",
        },
        partitionKey);
}
