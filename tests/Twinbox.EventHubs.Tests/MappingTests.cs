using System.Text;
using Azure.Messaging.EventHubs;
using Twinbox.Transport;

namespace Twinbox.EventHubs.Tests;

public sealed class MappingTests
{
    [Fact]
    public void ToEventData_CarriesBodyIdentityAndHeaders()
    {
        var message = new TransportMessage(
            "msg-1",
            "order-placed",
            "orders",
            Encoding.UTF8.GetBytes("{}"),
            "application/json",
            new Dictionary<string, string> { ["traceparent"] = "00-abc-def-01", [TransportHeaders.MessageId] = "stale" },
            "customer-1");

        var data = EventHubsMapping.ToEventData(message);

        Assert.Equal("{}", data.EventBody.ToString());
        Assert.Equal("msg-1", data.MessageId);
        Assert.Equal("application/json", data.ContentType);
        Assert.Equal("msg-1", data.Properties[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", data.Properties[TransportHeaders.MessageName]);
        Assert.Equal("00-abc-def-01", data.Properties["traceparent"]);
    }

    [Fact]
    public void ToIncomingMessage_ReadsIdentityHeadersAndPartitionKey()
    {
        var data = Received(
            new Dictionary<string, object>
            {
                [TransportHeaders.MessageId] = "msg-1",
                [TransportHeaders.MessageName] = "order-placed",
                ["attempts"] = 3,
                ["timestamp"] = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
            },
            partitionKey: "customer-1",
            sequenceNumber: 42);
        data.ContentType = "application/json";

        var message = EventHubsMapping.ToIncomingMessage(data, "orders", "3", deliveryAttempt: 2);

        Assert.Equal("msg-1", message.MessageId);
        Assert.Equal("order-placed", message.MessageName);
        Assert.Equal("orders", message.Source);
        Assert.Equal("application/json", message.ContentType);
        Assert.Equal("customer-1", message.PartitionKey);
        Assert.Equal(2, message.DeliveryAttempt);
        Assert.Equal("3", message.Headers["attempts"]);
        Assert.Equal("01/02/2026 03:04:05 +00:00", message.Headers["timestamp"]);
    }

    [Fact]
    public void ToIncomingMessage_RoundTripsTraceStateAndBaggage()
    {
        var message = new TransportMessage(
            "msg-1",
            "order-placed",
            "orders",
            Encoding.UTF8.GetBytes("{}"),
            "application/json",
            new Dictionary<string, string>
            {
                [TransportHeaders.TraceState] = "vendor=abc,other=xyz",
                [TransportHeaders.Baggage] = "tenant=acme%20corp,plan=gold",
            },
            "customer-1");

        var data = EventHubsMapping.ToEventData(message);
        var incoming = EventHubsMapping.ToIncomingMessage(Received(data.Properties, partitionKey: "customer-1"), "orders", "0");

        Assert.Equal("vendor=abc,other=xyz", (string)data.Properties[TransportHeaders.TraceState]);
        Assert.Equal("tenant=acme%20corp,plan=gold", (string)data.Properties[TransportHeaders.Baggage]);
        Assert.Equal("vendor=abc,other=xyz", incoming.Headers[TransportHeaders.TraceState]);
        Assert.Equal("tenant=acme%20corp,plan=gold", incoming.Headers[TransportHeaders.Baggage]);
    }

    [Fact]
    public void ToIncomingMessage_FallsBackToTheEventsMessageIdThenItsCoordinates()
    {
        var withMessageId = Received(new Dictionary<string, object>(), sequenceNumber: 7);
        withMessageId.MessageId = "native-id";
        var bare = Received(new Dictionary<string, object>(), sequenceNumber: 7);

        Assert.Equal("native-id", EventHubsMapping.ToIncomingMessage(withMessageId, "orders", "1").MessageId);
        var fallback = EventHubsMapping.ToIncomingMessage(bare, "orders", "1");
        Assert.Equal("orders:1:7", fallback.MessageId);
        Assert.Equal(string.Empty, fallback.MessageName);
        Assert.Equal("application/octet-stream", fallback.ContentType);
        Assert.Null(fallback.PartitionKey);
    }

    [Fact]
    public void ToDeadLetter_PinsTheIdAndRecordsTheFailureAndOrigin()
    {
        var data = Received(
            new Dictionary<string, object>
            {
                [TransportHeaders.MessageName] = "order-placed",
                [EventHubsMapping.ErrorProperty] = "an older failure",
            },
            partitionKey: "customer-1",
            sequenceNumber: 9);
        data.ContentType = "application/json";

        var copy = EventHubsMapping.ToDeadLetter(data, "orders", "2", new PermanentDeliveryException("cannot be handled"));

        Assert.Equal("orders:2:9", copy.MessageId);
        Assert.Equal("orders:2:9", copy.Properties[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", copy.Properties[TransportHeaders.MessageName]);
        Assert.Equal("PermanentDeliveryException: cannot be handled", copy.Properties[EventHubsMapping.ErrorProperty]);
        Assert.Equal("orders:2:9", copy.Properties[EventHubsMapping.OriginProperty]);
        Assert.Equal("application/json", copy.ContentType);
        Assert.Equal("{}", copy.EventBody.ToString());
    }

    [Fact]
    public void ToDeadLetter_TruncatesLongErrors()
    {
        var copy = EventHubsMapping.ToDeadLetter(
            Received(new Dictionary<string, object>()), "orders", "0", new InvalidOperationException(new string('x', 5000)));

        Assert.Equal(2000, ((string)copy.Properties[EventHubsMapping.ErrorProperty]).Length);
    }

    internal static EventData Received(IDictionary<string, object> properties, string? partitionKey = null, long sequenceNumber = 0) =>
        EventHubsModelFactory.EventData(
            new BinaryData("{}"),
            properties,
            new Dictionary<string, object>(),
            partitionKey,
            sequenceNumber,
            "0",
            DateTimeOffset.UnixEpoch);
}
