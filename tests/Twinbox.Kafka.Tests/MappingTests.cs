using System.Text;
using Confluent.Kafka;
using Twinbox.Transport;

namespace Twinbox.Kafka.Tests;

public sealed class MappingTests
{
    [Fact]
    public void ToKafkaMessage_KeysByPartitionKeyAndCarriesTheBody()
    {
        var message = KafkaMapping.ToKafkaMessage(Outgoing(partitionKey: "customer-7"));

        Assert.Equal("customer-7", message.Key);
        Assert.Equal("{}", Encoding.UTF8.GetString(message.Value));
    }

    [Fact]
    public void ToKafkaMessage_WithoutPartitionKey_HasNullKey() =>
        Assert.Null(KafkaMapping.ToKafkaMessage(Outgoing(partitionKey: null)).Key);

    [Fact]
    public void ToKafkaMessage_WritesTransportHeadersAndIdentityAsUtf8()
    {
        var message = KafkaMapping.ToKafkaMessage(Outgoing(partitionKey: null));

        Assert.Equal("00-abc-def-01", Header(message, TransportHeaders.TraceParent));
        Assert.Equal("tenant-ü", Header(message, TransportHeaders.TenantId));
        Assert.Equal("msg-1", Header(message, TransportHeaders.MessageId));
        Assert.Equal("order-placed", Header(message, TransportHeaders.MessageName));
        Assert.Equal("application/json", Header(message, KafkaMapping.ContentTypeHeader));
    }

    [Fact]
    public void ToKafkaMessage_LetsMessagePropertiesWinOverSameNamedHeaders()
    {
        var outgoing = Outgoing(partitionKey: null) with
        {
            Headers = new Dictionary<string, string>
            {
                [TransportHeaders.MessageId] = "stale",
                [KafkaMapping.ContentTypeHeader] = "text/plain",
            },
        };

        var message = KafkaMapping.ToKafkaMessage(outgoing);

        Assert.Single(message.Headers, h => h.Key == TransportHeaders.MessageId);
        Assert.Equal("msg-1", Header(message, TransportHeaders.MessageId));
        Assert.Equal("application/json", Header(message, KafkaMapping.ContentTypeHeader));
    }

    [Fact]
    public void ToIncomingMessage_RoundTripsAnOutgoingMessage()
    {
        var outgoing = Outgoing(partitionKey: "customer-7");

        var incoming = KafkaMapping.ToIncomingMessage(Record(KafkaMapping.ToKafkaMessage(outgoing)));

        Assert.Equal("msg-1", incoming.MessageId);
        Assert.Equal("order-placed", incoming.MessageName);
        Assert.Equal("orders", incoming.Source);
        Assert.Equal("application/json", incoming.ContentType);
        Assert.Equal("customer-7", incoming.PartitionKey);
        Assert.Equal("{}", Encoding.UTF8.GetString(incoming.Body.Span));
        Assert.Equal("00-abc-def-01", incoming.Headers[TransportHeaders.TraceParent]);
        Assert.Equal("tenant-ü", incoming.Headers[TransportHeaders.TenantId]);
        Assert.Equal(1, incoming.DeliveryAttempt);
    }

    [Fact]
    public void ToIncomingMessage_RoundTripsTraceStateAndBaggage()
    {
        var message = KafkaMapping.ToKafkaMessage(Outgoing(partitionKey: null) with
        {
            Headers = new Dictionary<string, string>
            {
                [TransportHeaders.TraceState] = "vendor=abc,other=xyz",
                [TransportHeaders.Baggage] = "tenant=acme%20corp,plan=gold",
            },
        });

        var incoming = KafkaMapping.ToIncomingMessage(Record(message));

        Assert.Equal("vendor=abc,other=xyz", Header(message, TransportHeaders.TraceState));
        Assert.Equal("tenant=acme%20corp,plan=gold", Header(message, TransportHeaders.Baggage));
        Assert.Equal("vendor=abc,other=xyz", incoming.Headers[TransportHeaders.TraceState]);
        Assert.Equal("tenant=acme%20corp,plan=gold", incoming.Headers[TransportHeaders.Baggage]);
    }

    [Fact]
    public void ToIncomingMessage_WithoutIdHeader_FallsBackToRecordCoordinates()
    {
        var incoming = KafkaMapping.ToIncomingMessage(Record(new Message<string?, byte[]> { Value = [1], Headers = [] }, partition: 2, offset: 42));

        Assert.Equal("orders:2:42", incoming.MessageId);
        Assert.Equal(string.Empty, incoming.MessageName);
        Assert.Equal("application/octet-stream", incoming.ContentType);
        Assert.Null(incoming.PartitionKey);
    }

    [Fact]
    public void ToIncomingMessage_FallbackIdIsStableAcrossRedeliveries()
    {
        var first = KafkaMapping.ToIncomingMessage(Record(new Message<string?, byte[]> { Value = [1] }, partition: 1, offset: 7));
        var redelivered = KafkaMapping.ToIncomingMessage(Record(new Message<string?, byte[]> { Value = [1] }, partition: 1, offset: 7), deliveryAttempt: 3);

        Assert.Equal(first.MessageId, redelivered.MessageId);
        Assert.Equal(3, redelivered.DeliveryAttempt);
    }

    [Fact]
    public void ToIncomingMessage_EmptyIdHeader_FallsBackToRecordCoordinates()
    {
        var headers = new Headers { { TransportHeaders.MessageId, [] } };

        var incoming = KafkaMapping.ToIncomingMessage(Record(new Message<string?, byte[]> { Value = [1], Headers = headers }, offset: 3));

        Assert.Equal("orders:0:3", incoming.MessageId);
    }

    [Fact]
    public void ToIncomingMessage_TombstoneHasEmptyBodyAndNullHeadersAreSkipped()
    {
        var headers = new Headers { { "empty", null }, { "tenant", Encoding.UTF8.GetBytes("a") }, { "tenant", Encoding.UTF8.GetBytes("b") } };

        var incoming = KafkaMapping.ToIncomingMessage(Record(new Message<string?, byte[]> { Key = "k", Value = null!, Headers = headers }));

        Assert.True(incoming.Body.IsEmpty);
        Assert.False(incoming.Headers.ContainsKey("empty"));
        Assert.Equal("b", incoming.Headers["tenant"]);
    }

    [Fact]
    public void ToDeadLetter_KeepsTheRecordAndAddsFailureDetails()
    {
        var original = KafkaMapping.ToKafkaMessage(Outgoing(partitionKey: "customer-7"));

        var copy = KafkaMapping.ToDeadLetter(Record(original, partition: 1, offset: 9), new PermanentDeliveryException("bad payload"));

        Assert.Equal("customer-7", copy.Key);
        Assert.Same(original.Value, copy.Value);
        Assert.Equal("msg-1", Header(copy, TransportHeaders.MessageId));
        Assert.Equal("order-placed", Header(copy, TransportHeaders.MessageName));
        Assert.Equal("PermanentDeliveryException: bad payload", Header(copy, KafkaMapping.ErrorHeader));
        Assert.Equal("orders:1:9", Header(copy, KafkaMapping.OriginHeader));
    }

    [Fact]
    public void ToDeadLetter_PinsTheFallbackIdAndReplacesEarlierFailureHeaders()
    {
        var headers = new Headers
        {
            { KafkaMapping.ErrorHeader, Encoding.UTF8.GetBytes("old") },
            { KafkaMapping.OriginHeader, Encoding.UTF8.GetBytes("elsewhere:0:0") },
        };

        var copy = KafkaMapping.ToDeadLetter(Record(new Message<string?, byte[]> { Value = [1], Headers = headers }, offset: 4), new InvalidOperationException("new"));

        Assert.Equal("orders:0:4", Header(copy, TransportHeaders.MessageId));
        Assert.Single(copy.Headers, h => h.Key == KafkaMapping.ErrorHeader);
        Assert.Equal("InvalidOperationException: new", Header(copy, KafkaMapping.ErrorHeader));
        Assert.Equal("orders:0:4", Header(copy, KafkaMapping.OriginHeader));
    }

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

    private static ConsumeResult<string?, byte[]> Record(Message<string?, byte[]> message, int partition = 0, long offset = 0) => new()
    {
        Topic = "orders",
        Partition = new Partition(partition),
        Offset = new Offset(offset),
        Message = message,
    };

    private static string Header(Message<string?, byte[]> message, string name) =>
        Encoding.UTF8.GetString(message.Headers.GetLastBytes(name));
}
