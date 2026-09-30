using System.Globalization;
using System.Text;
using Azure.Messaging.ServiceBus;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus.Tests;

public sealed class MappingTests
{
    [Fact]
    public void ToServiceBusMessage_MapsIdentityAndHeaders()
    {
        var mapped = AzureServiceBusMapping.ToServiceBusMessage(Outgoing(partitionKey: null), sendSessionIds: false);

        Assert.Equal("msg-1", mapped.MessageId);
        Assert.Equal("OrderPlaced", mapped.Subject);
        Assert.Equal("application/json", mapped.ContentType);
        Assert.Equal("""{"orderId":1}""", mapped.Body.ToString());
        Assert.Equal("tenant-a", mapped.ApplicationProperties["tenant"]);
        Assert.Equal("00-abc-def-01", mapped.ApplicationProperties[TransportHeaders.TraceParent]);
        Assert.False(mapped.ApplicationProperties.ContainsKey(TransportHeaders.PartitionKey));
        Assert.Null(mapped.SessionId);
    }

    [Fact]
    public void ToServiceBusMessage_WithoutSessions_CarriesPartitionKeyAsHeaderOnly()
    {
        var mapped = AzureServiceBusMapping.ToServiceBusMessage(Outgoing(partitionKey: "order-1"), sendSessionIds: false);

        Assert.Equal("order-1", mapped.ApplicationProperties[TransportHeaders.PartitionKey]);
        Assert.Null(mapped.SessionId);
    }

    [Fact]
    public void ToServiceBusMessage_WithSessionIdsButNoPartitionKey_UsesTheMessageId()
    {
        var mapped = AzureServiceBusMapping.ToServiceBusMessage(Outgoing(partitionKey: null), sendSessionIds: true);

        Assert.Equal("msg-1", mapped.SessionId);
        Assert.False(mapped.ApplicationProperties.ContainsKey(TransportHeaders.PartitionKey));
    }

    [Fact]
    public void ToIncomingMessage_WithFallbackSessionId_HasNoPartitionKey()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(messageId: "msg-7", sessionId: "msg-7", deliveryCount: 1);

        Assert.Null(AzureServiceBusMapping.ToIncomingMessage(received, "orders").PartitionKey);
    }

    [Fact]
    public void ToServiceBusMessage_WithSessions_UsesPartitionKeyAsSessionId()
    {
        var mapped = AzureServiceBusMapping.ToServiceBusMessage(Outgoing(partitionKey: "order-1"), sendSessionIds: true);

        Assert.Equal("order-1", mapped.ApplicationProperties[TransportHeaders.PartitionKey]);
        Assert.Equal("order-1", mapped.SessionId);
    }

    [Fact]
    public void ToIncomingMessage_MapsBrokerProperties()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"orderId":2}"""),
            messageId: "msg-2",
            subject: "OrderPlaced",
            contentType: "application/json",
            properties: new Dictionary<string, object> { ["tenant"] = "tenant-b", [TransportHeaders.PartitionKey] = "order-2" },
            deliveryCount: 3);

        var incoming = AzureServiceBusMapping.ToIncomingMessage(received, "orders");

        Assert.Equal("msg-2", incoming.MessageId);
        Assert.Equal("OrderPlaced", incoming.MessageName);
        Assert.Equal("orders", incoming.Source);
        Assert.Equal("""{"orderId":2}""", Encoding.UTF8.GetString(incoming.Body.Span));
        Assert.Equal("application/json", incoming.ContentType);
        Assert.Equal("tenant-b", incoming.Headers["tenant"]);
        Assert.Equal(3, incoming.DeliveryAttempt);
        Assert.Equal("order-2", incoming.PartitionKey);
    }

    [Fact]
    public void ToIncomingMessage_WithoutSubject_FallsBackToNameHeader()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            messageId: "msg-3",
            properties: new Dictionary<string, object> { [TransportHeaders.MessageName] = "invoice-issued.v1" },
            deliveryCount: 1);

        var incoming = AzureServiceBusMapping.ToIncomingMessage(received, "billing/Subscriptions/invoices");

        Assert.Equal("invoice-issued.v1", incoming.MessageName);
        Assert.Equal("application/octet-stream", incoming.ContentType);
    }

    [Fact]
    public void ToIncomingMessage_WithoutAnyName_LeavesNameEmptyForThePipelineToReject()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(messageId: "msg-4", deliveryCount: 1);

        Assert.Equal(string.Empty, AzureServiceBusMapping.ToIncomingMessage(received, "orders").MessageName);
    }

    [Fact]
    public void ToIncomingMessage_FormatsNonStringPropertiesInvariantly()
    {
        var culture = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("de-DE");
        try
        {
            var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
                messageId: "msg-5",
                properties: new Dictionary<string, object> { ["amount"] = 12.5, ["count"] = 3, ["flag"] = true },
                deliveryCount: 1);

            var headers = AzureServiceBusMapping.ToIncomingMessage(received, "orders").Headers;

            Assert.Equal("12.5", headers["amount"]);
            Assert.Equal("3", headers["count"]);
            Assert.Equal("True", headers["flag"]);
        }
        finally
        {
            CultureInfo.CurrentCulture = culture;
        }
    }

    [Fact]
    public void ToIncomingMessage_WithSessionButNoHeader_UsesSessionIdAsPartitionKey()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(messageId: "msg-6", sessionId: "order-6", deliveryCount: 1);

        Assert.Equal("order-6", AzureServiceBusMapping.ToIncomingMessage(received, "orders").PartitionKey);
    }

    [Fact]
    public void SentMessage_RoundTripsToTheSameIncomingMessage()
    {
        var outgoing = Outgoing(partitionKey: "order-1");
        var sent = AzureServiceBusMapping.ToServiceBusMessage(outgoing, sendSessionIds: true);
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: sent.Body,
            messageId: sent.MessageId,
            sessionId: sent.SessionId,
            subject: sent.Subject,
            contentType: sent.ContentType,
            properties: sent.ApplicationProperties,
            deliveryCount: 1);

        var incoming = AzureServiceBusMapping.ToIncomingMessage(received, outgoing.Destination);

        Assert.Equal(outgoing.MessageId, incoming.MessageId);
        Assert.Equal(outgoing.MessageName, incoming.MessageName);
        Assert.Equal(outgoing.ContentType, incoming.ContentType);
        Assert.Equal(outgoing.PartitionKey, incoming.PartitionKey);
        Assert.Equal(outgoing.Body.ToArray(), incoming.Body.ToArray());
        foreach (var header in outgoing.Headers)
        {
            Assert.Equal(header.Value, incoming.Headers[header.Key]);
        }
    }

    internal static TransportMessage Outgoing(string? partitionKey) => new(
        "msg-1",
        "OrderPlaced",
        "orders",
        Encoding.UTF8.GetBytes("""{"orderId":1}"""),
        "application/json",
        new Dictionary<string, string> { ["tenant"] = "tenant-a", [TransportHeaders.TraceParent] = "00-abc-def-01" },
        partitionKey);
}
