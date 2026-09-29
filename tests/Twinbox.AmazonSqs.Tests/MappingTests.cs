using System.Text;
using Amazon.SQS.Model;
using Twinbox.Transport;

namespace Twinbox.AmazonSqs.Tests;

public sealed class MappingTests
{
    [Fact]
    public void ToOutgoing_CarriesIdentityAsAttributesAndJsonAsText()
    {
        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(headers: new() { ["traceparent"] = "00-abc-01" }), fifo: false);

        Assert.Equal("""{"orderId":1}""", outgoing.Body);
        Assert.Equal("m-1", outgoing.Attributes[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", outgoing.Attributes[TransportHeaders.MessageName]);
        Assert.Equal("application/json", outgoing.Attributes[AmazonSqsMapping.ContentTypeAttribute]);
        Assert.Equal("customer-1", outgoing.Attributes[TransportHeaders.PartitionKey]);
        Assert.Equal("00-abc-01", outgoing.Attributes["traceparent"]);
        Assert.False(outgoing.Attributes.ContainsKey(AmazonSqsMapping.BodyEncodingAttribute));
        Assert.Null(outgoing.GroupId);
        Assert.Null(outgoing.DeduplicationId);
    }

    [Theory]
    [InlineData("application/octet-stream")]
    [InlineData("application/x-protobuf")]
    public void ToOutgoing_Base64EncodesNonTextualBodies(string contentType)
    {
        byte[] body = [0, 1, 2, 255];

        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(body: body, contentType: contentType), fifo: false);

        Assert.Equal(Convert.ToBase64String(body), outgoing.Body);
        Assert.Equal(AmazonSqsMapping.Base64Encoding, outgoing.Attributes[AmazonSqsMapping.BodyEncodingAttribute]);
    }

    [Theory]
    [InlineData(new byte[] { 0xC3, 0x28 })]
    [InlineData(new byte[] { (byte)'a', 0x00, (byte)'b' })]
    public void ToOutgoing_Base64EncodesTextBodiesSqsWouldReject(byte[] body)
    {
        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(body: body, contentType: "text/plain"), fifo: false);

        Assert.Equal(AmazonSqsMapping.Base64Encoding, outgoing.Attributes[AmazonSqsMapping.BodyEncodingAttribute]);
    }

    [Theory]
    [InlineData("text/plain; charset=utf-8")]
    [InlineData("application/cloudevents+json")]
    [InlineData("application/xml")]
    public void ToOutgoing_KeepsTextualBodiesReadable(string contentType)
    {
        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(contentType: contentType), fifo: false);

        Assert.Equal("""{"orderId":1}""", outgoing.Body);
    }

    [Fact]
    public void ToOutgoing_KeepsWithinTheAttributeLimitByMovingOverflowIntoJson()
    {
        var headers = Enumerable.Range(1, 12).ToDictionary(i => $"x-{i:00}", i => $"v{i}");
        headers[TransportHeaders.TraceParent] = "00-trace-01";

        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(headers: headers), fifo: false);

        Assert.Equal(AmazonSqsMapping.MaxAttributes, outgoing.Attributes.Count);
        Assert.Equal("00-trace-01", outgoing.Attributes[TransportHeaders.TraceParent]);
        Assert.True(outgoing.Attributes.ContainsKey(AmazonSqsMapping.HeadersAttribute));
        var incoming = AmazonSqsMapping.ToIncoming(Received(outgoing), "orders");
        foreach (var (name, value) in headers)
        {
            Assert.Equal(value, incoming.Headers[name]);
        }

        Assert.False(incoming.Headers.ContainsKey(AmazonSqsMapping.HeadersAttribute));
    }

    [Fact]
    public void ToOutgoing_UsesIndividualAttributesWhenEverythingFits()
    {
        var headers = Enumerable.Range(1, 6).ToDictionary(i => $"x-{i}", i => $"v{i}");

        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(headers: headers), fifo: false);

        Assert.Equal(AmazonSqsMapping.MaxAttributes, outgoing.Attributes.Count);
        Assert.False(outgoing.Attributes.ContainsKey(AmazonSqsMapping.HeadersAttribute));
    }

    [Theory]
    [InlineData("has space")]
    [InlineData("AWS.reserved")]
    [InlineData("two..dots")]
    [InlineData(".leading")]
    public void ToOutgoing_MovesHeadersSqsCannotNameIntoJson(string name)
    {
        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(headers: new() { [name] = "v", ["empty"] = string.Empty }), fifo: false);

        Assert.False(outgoing.Attributes.ContainsKey(name));
        var incoming = AmazonSqsMapping.ToIncoming(Received(outgoing), "orders");
        Assert.Equal("v", incoming.Headers[name]);
        Assert.Equal(string.Empty, incoming.Headers["empty"]);
    }

    [Fact]
    public void ToOutgoing_ForFifo_GroupsByPartitionKeyAndDeduplicatesById()
    {
        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(), fifo: true);

        Assert.Equal("customer-1", outgoing.GroupId);
        Assert.Equal("m-1", outgoing.DeduplicationId);
    }

    [Fact]
    public void ToOutgoing_ForFifoWithoutPartitionKey_UsesOneSharedGroup()
    {
        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(partitionKey: null), fifo: true);

        Assert.Equal(AmazonSqsMapping.DefaultGroupId, outgoing.GroupId);
    }

    [Theory]
    [InlineData("customer 1")]
    [InlineData("kunde-ü")]
    public void FifoId_HashesValuesSqsWouldReject(string value)
    {
        var id = AmazonSqsMapping.FifoId(value);

        Assert.NotNull(id);
        Assert.Equal(64, id.Length);
        Assert.Equal(id, AmazonSqsMapping.FifoId(value));
    }

    [Fact]
    public void FifoId_HashesOverlongValues()
    {
        Assert.Equal(64, AmazonSqsMapping.FifoId(new string('a', 129))!.Length);
        Assert.Equal(new string('a', 128), AmazonSqsMapping.FifoId(new string('a', 128)));
    }

    [Fact]
    public void ToSendRequest_AndToPublishRequest_CarryTheSameMessage()
    {
        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(), fifo: true);

        var send = AmazonSqsMapping.ToSendRequest(outgoing, "https://sqs/orders.fifo");
        var publish = AmazonSqsMapping.ToPublishRequest(outgoing, "arn:aws:sns:us-east-1:000000000000:orders.fifo");

        Assert.Equal("https://sqs/orders.fifo", send.QueueUrl);
        Assert.Equal(outgoing.Body, send.MessageBody);
        Assert.Equal("customer-1", send.MessageGroupId);
        Assert.Equal("m-1", send.MessageDeduplicationId);
        Assert.All(send.MessageAttributes.Values, a => Assert.Equal("String", a.DataType));
        Assert.Equal("m-1", send.MessageAttributes[TransportHeaders.MessageId].StringValue);
        Assert.Equal(outgoing.Body, publish.Message);
        Assert.Equal("customer-1", publish.MessageGroupId);
        Assert.Equal("m-1", publish.MessageDeduplicationId);
        Assert.Equal("order-placed", publish.MessageAttributes[TransportHeaders.MessageName].StringValue);
    }

    [Fact]
    public void ToIncoming_RestoresTheSentMessage()
    {
        byte[] body = [9, 8, 7];
        var outgoing = AmazonSqsMapping.ToOutgoing(Transport(body: body, contentType: "application/octet-stream"), fifo: false);

        var incoming = AmazonSqsMapping.ToIncoming(Received(outgoing, receiveCount: "3"), "orders");

        Assert.Equal("m-1", incoming.MessageId);
        Assert.Equal("order-placed", incoming.MessageName);
        Assert.Equal("orders", incoming.Source);
        Assert.Equal(body, incoming.Body.ToArray());
        Assert.Equal("application/octet-stream", incoming.ContentType);
        Assert.Equal(3, incoming.DeliveryAttempt);
        Assert.Equal("customer-1", incoming.PartitionKey);
        Assert.False(incoming.Headers.ContainsKey(AmazonSqsMapping.BodyEncodingAttribute));
    }

    [Fact]
    public void ToIncoming_FallsBackToSqsIdentityForForeignMessages()
    {
        var message = new Message
        {
            MessageId = "sqs-id",
            Body = "hello",
            Attributes = new() { [AmazonSqsMapping.GroupIdAttribute] = "group-7" },
        };

        var incoming = AmazonSqsMapping.ToIncoming(message, "orders");

        Assert.Equal("sqs-id", incoming.MessageId);
        Assert.Equal(string.Empty, incoming.MessageName);
        Assert.Equal("application/octet-stream", incoming.ContentType);
        Assert.Equal(1, incoming.DeliveryAttempt);
        Assert.Equal("group-7", incoming.PartitionKey);
        Assert.Equal("hello", Encoding.UTF8.GetString(incoming.Body.Span));
    }

    [Fact]
    public void ToIncoming_UnwrapsAnSnsEnvelope()
    {
        var envelope = """
            {"Type":"Notification","MessageId":"sns-id","TopicArn":"arn:aws:sns:us-east-1:000000000000:orders",
             "Message":"{\"orderId\":4}",
             "MessageAttributes":{"twinbox-message-id":{"Type":"String","Value":"m-4"},"twinbox-message-name":{"Type":"String","Value":"order-placed"}}}
            """;

        var incoming = AmazonSqsMapping.ToIncoming(new Message { MessageId = "sqs-id", Body = envelope }, "orders");

        Assert.Equal("m-4", incoming.MessageId);
        Assert.Equal("order-placed", incoming.MessageName);
        Assert.Equal("""{"orderId":4}""", Encoding.UTF8.GetString(incoming.Body.Span));
    }

    [Fact]
    public void ToIncoming_SurvivesACorruptBase64Body()
    {
        var message = new Message
        {
            MessageId = "sqs-id",
            Body = "not base64!",
            MessageAttributes = new() { [AmazonSqsMapping.BodyEncodingAttribute] = new() { DataType = "String", StringValue = "base64" } },
        };

        var incoming = AmazonSqsMapping.ToIncoming(message, "orders");

        Assert.Equal("not base64!", Encoding.UTF8.GetString(incoming.Body.Span));
    }

    [Fact]
    public void ToDeadLetter_KeepsIdentityAndRecordsTheFailure()
    {
        var incoming = new IncomingMessage(
            "m-1", "order-placed", "orders", new byte[] { 1 }, "application/json", new Dictionary<string, string> { ["x"] = "y" }, 2, "k");

        var copy = AmazonSqsMapping.ToDeadLetter(incoming, new InvalidOperationException(new string('e', 5000)), "orders-dlq");

        Assert.Equal("m-1", copy.MessageId);
        Assert.Equal("orders-dlq", copy.Destination);
        Assert.Equal("k", copy.PartitionKey);
        Assert.Equal("y", copy.Headers["x"]);
        Assert.Equal("orders", copy.Headers[AmazonSqsMapping.OriginHeader]);
        Assert.StartsWith("InvalidOperationException: ", copy.Headers[AmazonSqsMapping.ErrorHeader], StringComparison.Ordinal);
        Assert.Equal(2000, copy.Headers[AmazonSqsMapping.ErrorHeader].Length);
    }

    [Theory]
    [InlineData("orders.fifo", true)]
    [InlineData("https://sqs.eu-west-1.amazonaws.com/1/orders.fifo", true)]
    [InlineData("orders", false)]
    [InlineData("orders.FIFO", false)]
    public void IsFifo_FollowsTheNamingRule(string name, bool expected) => Assert.Equal(expected, AmazonSqsMapping.IsFifo(name));

    private static TransportMessage Transport(
        byte[]? body = null,
        string contentType = "application/json",
        Dictionary<string, string>? headers = null,
        string? partitionKey = "customer-1") =>
        new("m-1", "order-placed", "orders", body ?? Encoding.UTF8.GetBytes("""{"orderId":1}"""), contentType, headers ?? [], partitionKey);

    private static Message Received(OutgoingMessage outgoing, string receiveCount = "1") => new()
    {
        MessageId = "sqs-id",
        Body = outgoing.Body,
        MessageAttributes = outgoing.Attributes.ToDictionary(a => a.Key, a => new MessageAttributeValue { DataType = "String", StringValue = a.Value }),
        Attributes = new() { [AmazonSqsMapping.ReceiveCountAttribute] = receiveCount },
    };
}
