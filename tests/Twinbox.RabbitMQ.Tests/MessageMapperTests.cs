using System.Text;
using RabbitMQ.Client;
using Twinbox.RabbitMQ;
using Twinbox.Transport;

namespace Twinbox.RabbitMQ.Tests;

public sealed class MessageMapperTests
{
    [Fact]
    public void ToProperties_MapsIdentityAndPersistence()
    {
        var properties = RabbitMQMessageMapper.ToProperties(Outgoing(partitionKey: null));

        Assert.Equal("msg-1", properties.MessageId);
        Assert.Equal("order-placed", properties.Type);
        Assert.Equal("application/json", properties.ContentType);
        Assert.True(properties.Persistent);
    }

    [Fact]
    public void ToProperties_CopiesHeadersAndAddsPartitionKey()
    {
        var properties = RabbitMQMessageMapper.ToProperties(Outgoing(partitionKey: "customer-7"));

        Assert.NotNull(properties.Headers);
        Assert.Equal("00-abc-def-01", properties.Headers[TransportHeaders.TraceParent]);
        Assert.Equal("customer-7", properties.Headers[TransportHeaders.PartitionKey]);
    }

    [Fact]
    public void ToIncoming_RoundTripsTraceStateAndBaggage()
    {
        var outgoing = Outgoing(partitionKey: null) with
        {
            Headers = new Dictionary<string, string>
            {
                [TransportHeaders.TraceState] = "vendor=abc,other=xyz",
                [TransportHeaders.Baggage] = "tenant=acme%20corp,plan=gold",
            },
        };
        var sent = RabbitMQMessageMapper.ToProperties(outgoing);
        Assert.NotNull(sent.Headers);

        // AMQP delivers string headers back as UTF-8 byte arrays.
        var received = new BasicProperties
        {
            MessageId = sent.MessageId,
            Type = sent.Type,
            ContentType = sent.ContentType,
            Headers = sent.Headers.ToDictionary(h => h.Key, h => (object?)Encoding.UTF8.GetBytes((string)h.Value!)),
        };

        var incoming = RabbitMQMessageMapper.ToIncoming("orders", received, outgoing.Body, redelivered: false);

        Assert.Equal("vendor=abc,other=xyz", sent.Headers[TransportHeaders.TraceState]);
        Assert.Equal("tenant=acme%20corp,plan=gold", sent.Headers[TransportHeaders.Baggage]);
        Assert.Equal("vendor=abc,other=xyz", incoming.Headers[TransportHeaders.TraceState]);
        Assert.Equal("tenant=acme%20corp,plan=gold", incoming.Headers[TransportHeaders.Baggage]);
    }

    [Fact]
    public void ToProperties_OmitsPartitionKeyWhenUnset()
    {
        var properties = RabbitMQMessageMapper.ToProperties(Outgoing(partitionKey: null));

        Assert.NotNull(properties.Headers);
        Assert.False(properties.Headers.ContainsKey(TransportHeaders.PartitionKey));
    }

    [Fact]
    public void ToIncoming_DecodesPropertiesAndHeaders()
    {
        var properties = new BasicProperties
        {
            MessageId = "msg-2",
            Type = "order-placed",
            ContentType = "application/json",
            Headers = new Dictionary<string, object?>
            {
                [TransportHeaders.PartitionKey] = Encoding.UTF8.GetBytes("customer-9"),
                ["tenant"] = Encoding.UTF8.GetBytes("acme"),
                ["priority"] = 5,
                ["x-death"] = new List<object?>(),
            },
        };

        var incoming = RabbitMQMessageMapper.ToIncoming("orders", properties, Encoding.UTF8.GetBytes("{}"), redelivered: false);

        Assert.Equal("msg-2", incoming.MessageId);
        Assert.Equal("order-placed", incoming.MessageName);
        Assert.Equal("orders", incoming.Source);
        Assert.Equal("application/json", incoming.ContentType);
        Assert.Equal("customer-9", incoming.PartitionKey);
        Assert.Equal("acme", incoming.Headers["tenant"]);
        Assert.Equal("5", incoming.Headers["priority"]);
        Assert.False(incoming.Headers.ContainsKey("x-death"));
        Assert.Equal("{}", Encoding.UTF8.GetString(incoming.Body.Span));
        Assert.Equal(1, incoming.DeliveryAttempt);
    }

    [Fact]
    public void ToIncoming_FallsBackToHeadersForIdAndName()
    {
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?>
            {
                [TransportHeaders.MessageId] = Encoding.UTF8.GetBytes("msg-3"),
                [TransportHeaders.MessageName] = Encoding.UTF8.GetBytes("order-shipped"),
            },
        };

        var incoming = RabbitMQMessageMapper.ToIncoming("orders", properties, ReadOnlyMemory<byte>.Empty, redelivered: false);

        Assert.Equal("msg-3", incoming.MessageId);
        Assert.Equal("order-shipped", incoming.MessageName);
        Assert.Equal("application/octet-stream", incoming.ContentType);
    }

    [Theory]
    [InlineData(2L, false, 3)]
    [InlineData(0L, true, 1)]
    public void ToIncoming_DeliveryAttemptFollowsDeliveryCount(long deliveryCount, bool redelivered, int expected)
    {
        var properties = new BasicProperties
        {
            MessageId = "msg-4",
            Headers = new Dictionary<string, object?> { [RabbitMQMessageMapper.DeliveryCountHeader] = deliveryCount },
        };

        var incoming = RabbitMQMessageMapper.ToIncoming("orders", properties, ReadOnlyMemory<byte>.Empty, redelivered);

        Assert.Equal(expected, incoming.DeliveryAttempt);
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    public void ToIncoming_WithoutDeliveryCount_UsesRedeliveredFlag(bool redelivered, int expected)
    {
        var incoming = RabbitMQMessageMapper.ToIncoming("orders", new BasicProperties { MessageId = "msg-5" }, ReadOnlyMemory<byte>.Empty, redelivered);

        Assert.Equal(expected, incoming.DeliveryAttempt);
    }

    [Fact]
    public void ToIncoming_WithoutMessageId_LeavesItForHeaderProfiles()
    {
        var incoming = RabbitMQMessageMapper.ToIncoming("orders", new BasicProperties { Type = "order-placed" }, ReadOnlyMemory<byte>.Empty, false);

        Assert.Equal(string.Empty, incoming.MessageId);
    }

    [Fact]
    public void QueueArguments_DeclareQuorumQueueWithDeadLettering()
    {
        var arguments = RabbitMQTopology.QueueArguments("orders", deliveryLimit: 7);

        Assert.Equal("quorum", arguments["x-queue-type"]);
        Assert.Equal(7, arguments["x-delivery-limit"]);
        Assert.Equal("twinbox.dead-letter", arguments["x-dead-letter-exchange"]);
        Assert.Equal("orders.dlq", arguments["x-dead-letter-routing-key"]);
    }

    private static TransportMessage Outgoing(string? partitionKey) => new(
        "msg-1",
        "order-placed",
        "sales",
        Encoding.UTF8.GetBytes("{}"),
        "application/json",
        new Dictionary<string, string> { [TransportHeaders.TraceParent] = "00-abc-def-01" },
        partitionKey);
}
