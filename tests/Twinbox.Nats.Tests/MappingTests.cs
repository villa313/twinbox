using System.Text;
using NATS.Client.Core;
using Twinbox.Transport;

namespace Twinbox.Nats.Tests;

public sealed class MappingTests
{
    [Fact]
    public void ToHeaders_SetsNatsMsgIdToTheMessageId() =>
        Assert.Equal("msg-1", NatsMapping.ToHeaders(Outgoing(partitionKey: null))[NatsMapping.MsgIdHeader].ToString());

    [Fact]
    public void ToHeaders_CarriesIdentityContentTypeAndTransportHeaders()
    {
        var headers = NatsMapping.ToHeaders(Outgoing(partitionKey: "customer-7"));

        Assert.Equal("msg-1", headers[TransportHeaders.MessageId].ToString());
        Assert.Equal("order-placed", headers[TransportHeaders.MessageName].ToString());
        Assert.Equal("application/json", headers[NatsMapping.ContentTypeHeader].ToString());
        Assert.Equal("customer-7", headers[TransportHeaders.PartitionKey].ToString());
        Assert.Equal("00-abc-def-01", headers[TransportHeaders.TraceParent].ToString());
        Assert.Equal("tenant-a", headers[TransportHeaders.TenantId].ToString());
    }

    [Fact]
    public void ToHeaders_WithoutPartitionKey_OmitsItsHeader() =>
        Assert.False(NatsMapping.ToHeaders(Outgoing(partitionKey: null)).ContainsKey(TransportHeaders.PartitionKey));

    [Fact]
    public void ToHeaders_LetsMessagePropertiesWinOverSameNamedHeaders()
    {
        var outgoing = Outgoing(partitionKey: null) with
        {
            Headers = new Dictionary<string, string>
            {
                [TransportHeaders.MessageId] = "stale",
                [NatsMapping.MsgIdHeader] = "stale",
                [NatsMapping.ContentTypeHeader] = "text/plain",
            },
        };

        var headers = NatsMapping.ToHeaders(outgoing);

        Assert.Equal("msg-1", headers[TransportHeaders.MessageId].ToString());
        Assert.Equal("msg-1", headers[NatsMapping.MsgIdHeader].ToString());
        Assert.Equal("application/json", headers[NatsMapping.ContentTypeHeader].ToString());
    }

    [Fact]
    public void ToIncomingMessage_RoundTripsAnOutgoingMessage()
    {
        var outgoing = Outgoing(partitionKey: "customer-7");

        var incoming = NatsMapping.ToIncomingMessage("orders.placed", outgoing.Body, NatsMapping.ToHeaders(outgoing), 3, "ORDERS:12");

        Assert.Equal("msg-1", incoming.MessageId);
        Assert.Equal("order-placed", incoming.MessageName);
        Assert.Equal("orders.placed", incoming.Source);
        Assert.Equal("application/json", incoming.ContentType);
        Assert.Equal("customer-7", incoming.PartitionKey);
        Assert.Equal(3, incoming.DeliveryAttempt);
        Assert.Equal("{}", Encoding.UTF8.GetString(incoming.Body.Span));
        Assert.Equal("00-abc-def-01", incoming.Headers[TransportHeaders.TraceParent]);
    }

    [Fact]
    public void ToIncomingMessage_WithOnlyNatsMsgId_UsesIt()
    {
        var headers = new NatsHeaders { [NatsMapping.MsgIdHeader] = "external-9" };

        Assert.Equal("external-9", NatsMapping.ToIncomingMessage("orders.placed", Array.Empty<byte>(), headers, 1, "ORDERS:4").MessageId);
    }

    [Fact]
    public void ToIncomingMessage_WithoutHeaders_FallsBackToStreamCoordinates()
    {
        var incoming = NatsMapping.ToIncomingMessage("orders.placed", new byte[] { 1 }, null, 0, "ORDERS:42");

        Assert.Equal("ORDERS:42", incoming.MessageId);
        Assert.Equal(string.Empty, incoming.MessageName);
        Assert.Equal("application/octet-stream", incoming.ContentType);
        Assert.Null(incoming.PartitionKey);
        Assert.Equal(1, incoming.DeliveryAttempt);
    }

    [Fact]
    public void ToIncomingMessage_RepeatedHeader_KeepsTheLastValue()
    {
        var headers = new NatsHeaders { ["tenant"] = new(["a", "b"]) };

        Assert.Equal("b", NatsMapping.ToIncomingMessage("s", Array.Empty<byte>(), headers, 1, "S:1").Headers["tenant"]);
    }

    [Fact]
    public void ToDeadLetterHeaders_KeepsIdentityAndAddsFailureDetails()
    {
        var outgoing = Outgoing(partitionKey: "customer-7");
        var incoming = NatsMapping.ToIncomingMessage("orders.placed", outgoing.Body, NatsMapping.ToHeaders(outgoing), 2, "ORDERS:9");

        var headers = NatsMapping.ToDeadLetterHeaders(incoming, "ORDERS:9", new PermanentDeliveryException("bad\r\npayload"));

        Assert.Equal("msg-1", headers[TransportHeaders.MessageId].ToString());
        Assert.Equal("order-placed", headers[TransportHeaders.MessageName].ToString());
        Assert.Equal("customer-7", headers[TransportHeaders.PartitionKey].ToString());
        Assert.Equal("ORDERS:9", headers[NatsMapping.OriginHeader].ToString());
        Assert.Equal("dead-letter:ORDERS:9", headers[NatsMapping.MsgIdHeader].ToString());
        Assert.Equal("PermanentDeliveryException: bad payload", headers[NatsMapping.ErrorHeader].ToString());
    }

    [Fact]
    public void ToDeadLetterHeaders_TruncatesLongErrors()
    {
        var incoming = NatsMapping.ToIncomingMessage("s", Array.Empty<byte>(), null, 1, "S:1");

        var headers = NatsMapping.ToDeadLetterHeaders(incoming, "S:1", new InvalidOperationException(new string('x', 5000)));

        Assert.Equal(2000, headers[NatsMapping.ErrorHeader].ToString().Length);
    }

    [Theory]
    [InlineData(1, 100)]
    [InlineData(2, 200)]
    [InlineData(4, 800)]
    [InlineData(5, 1000)]
    [InlineData(500, 1000)]
    public void RetryDelay_DoublesUpToItsCap(int attempt, int expectedMilliseconds)
    {
        var options = new NatsOptions { RetryDelay = TimeSpan.FromMilliseconds(100), MaxRetryDelay = TimeSpan.FromSeconds(1) };

        Assert.Equal(TimeSpan.FromMilliseconds(expectedMilliseconds), NatsConsumerService.RetryDelay(options, attempt));
    }

    private static TransportMessage Outgoing(string? partitionKey) => new(
        "msg-1",
        "order-placed",
        "orders.placed",
        "{}"u8.ToArray(),
        "application/json",
        new Dictionary<string, string>
        {
            [TransportHeaders.TraceParent] = "00-abc-def-01",
            [TransportHeaders.TenantId] = "tenant-a",
        },
        partitionKey);
}
