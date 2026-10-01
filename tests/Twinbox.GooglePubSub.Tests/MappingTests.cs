using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Twinbox.Transport;
using Encoding = System.Text.Encoding;

namespace Twinbox.GooglePubSub.Tests;

public sealed class MappingTests
{
    [Fact]
    public void ToPubsubMessage_CarriesBodyIdentityAndHeadersAsAttributes()
    {
        var message = GooglePubSubMapping.ToPubsubMessage(Outgoing(partitionKey: null), ordering: false);

        Assert.Equal("{}", message.Data.ToStringUtf8());
        Assert.Equal("msg-1", message.Attributes[TransportHeaders.MessageId]);
        Assert.Equal("order-placed", message.Attributes[TransportHeaders.MessageName]);
        Assert.Equal("application/json", message.Attributes[GooglePubSubMapping.ContentTypeAttribute]);
        Assert.Equal("00-abc-def-01", message.Attributes[TransportHeaders.TraceParent]);
        Assert.Equal("tenant-ü", message.Attributes[TransportHeaders.TenantId]);
        Assert.False(message.Attributes.ContainsKey(TransportHeaders.PartitionKey));
        Assert.Equal(string.Empty, message.OrderingKey);
    }

    [Fact]
    public void ToPubsubMessage_LetsMessagePropertiesWinOverSameNamedHeaders()
    {
        var outgoing = Outgoing(partitionKey: null) with
        {
            Headers = new Dictionary<string, string>
            {
                [TransportHeaders.MessageId] = "stale",
                [GooglePubSubMapping.ContentTypeAttribute] = "text/plain",
            },
        };

        var message = GooglePubSubMapping.ToPubsubMessage(outgoing, ordering: false);

        Assert.Equal("msg-1", message.Attributes[TransportHeaders.MessageId]);
        Assert.Equal("application/json", message.Attributes[GooglePubSubMapping.ContentTypeAttribute]);
    }

    [Theory]
    [InlineData(true, "customer-7")]
    [InlineData(false, "")]
    public void ToPubsubMessage_UsesThePartitionKeyAsOrderingKeyOnlyWhenOrdering(bool ordering, string expectedOrderingKey)
    {
        var message = GooglePubSubMapping.ToPubsubMessage(Outgoing(partitionKey: "customer-7"), ordering);

        Assert.Equal(expectedOrderingKey, message.OrderingKey);
        Assert.Equal("customer-7", message.Attributes[TransportHeaders.PartitionKey]);
    }

    [Fact]
    public void ToIncoming_RoundTripsAnOutgoingMessage()
    {
        var published = GooglePubSubMapping.ToPubsubMessage(Outgoing(partitionKey: "customer-7"), ordering: true);
        published.MessageId = "broker-1";

        var incoming = GooglePubSubMapping.ToIncoming(published, "billing");

        Assert.Equal("msg-1", incoming.MessageId);
        Assert.Equal("order-placed", incoming.MessageName);
        Assert.Equal("billing", incoming.Source);
        Assert.Equal("application/json", incoming.ContentType);
        Assert.Equal("customer-7", incoming.PartitionKey);
        Assert.Equal("{}", Encoding.UTF8.GetString(incoming.Body.Span));
        Assert.Equal("00-abc-def-01", incoming.Headers[TransportHeaders.TraceParent]);
        Assert.Equal("tenant-ü", incoming.Headers[TransportHeaders.TenantId]);
        Assert.Equal(1, incoming.DeliveryAttempt);
    }

    [Fact]
    public void ToIncoming_RoundTripsTraceStateAndBaggage()
    {
        var published = GooglePubSubMapping.ToPubsubMessage(Outgoing(partitionKey: null) with
        {
            Headers = new Dictionary<string, string>
            {
                [TransportHeaders.TraceState] = "vendor=abc,other=xyz",
                [TransportHeaders.Baggage] = "tenant=acme%20corp,plan=gold",
            },
        }, ordering: false);
        published.MessageId = "broker-1";

        var incoming = GooglePubSubMapping.ToIncoming(published, "billing");

        Assert.Equal("vendor=abc,other=xyz", published.Attributes[TransportHeaders.TraceState]);
        Assert.Equal("tenant=acme%20corp,plan=gold", published.Attributes[TransportHeaders.Baggage]);
        Assert.Equal("vendor=abc,other=xyz", incoming.Headers[TransportHeaders.TraceState]);
        Assert.Equal("tenant=acme%20corp,plan=gold", incoming.Headers[TransportHeaders.Baggage]);
    }

    [Fact]
    public void ToIncoming_WithoutTwinboxAttributes_FallsBackToTheBrokerMessageId()
    {
        var incoming = GooglePubSubMapping.ToIncoming(new PubsubMessage { MessageId = "broker-9", Data = ByteString.CopyFrom(1, 2) }, "billing");

        Assert.Equal("broker-9", incoming.MessageId);
        Assert.Equal(string.Empty, incoming.MessageName);
        Assert.Equal("application/octet-stream", incoming.ContentType);
        Assert.Null(incoming.PartitionKey);
        Assert.Equal([1, 2], incoming.Body.ToArray());
    }

    [Fact]
    public void ToIncoming_PrefersTheOrderingKeyAsPartitionKey()
    {
        var message = new PubsubMessage { MessageId = "broker-1", OrderingKey = "customer-1" };
        message.Attributes[TransportHeaders.PartitionKey] = "stale";

        Assert.Equal("customer-1", GooglePubSubMapping.ToIncoming(message, "billing").PartitionKey);
    }

    [Fact]
    public void ToDeadLetter_KeepsIdentityAndAttachesTheFailure()
    {
        var original = new PubsubMessage { MessageId = "broker-1", Data = ByteString.CopyFromUtf8("{}"), OrderingKey = "customer-7" };
        original.Attributes["x-custom"] = "kept";
        var incoming = GooglePubSubMapping.ToIncoming(original, "billing");

        var copy = GooglePubSubMapping.ToDeadLetter(original, incoming, new PermanentDeliveryException(new string('x', 5000)));

        Assert.Equal("broker-1", copy.Attributes[TransportHeaders.MessageId]);
        Assert.Equal("customer-7", copy.Attributes[TransportHeaders.PartitionKey]);
        Assert.Equal("billing", copy.Attributes[GooglePubSubMapping.OriginAttribute]);
        Assert.Equal("kept", copy.Attributes["x-custom"]);
        Assert.StartsWith("PermanentDeliveryException: xxx", copy.Attributes[GooglePubSubMapping.ErrorAttribute], StringComparison.Ordinal);
        Assert.True(Encoding.UTF8.GetByteCount(copy.Attributes[GooglePubSubMapping.ErrorAttribute]) <= 1024);
        Assert.Equal(string.Empty, copy.OrderingKey);
        Assert.Equal("{}", copy.Data.ToStringUtf8());
    }

    [Theory]
    [InlineData("orders", "projects/p1/topics/orders")]
    [InlineData("projects/other/topics/audit", "projects/other/topics/audit")]
    public void ToTopicName_AcceptsIdsAndFullNames(string topic, string expected) =>
        Assert.Equal(expected, GooglePubSubMapping.ToTopicName(topic, "p1").ToString());

    [Theory]
    [InlineData("billing", "projects/p1/subscriptions/billing")]
    [InlineData("projects/other/subscriptions/audit", "projects/other/subscriptions/audit")]
    public void ToSubscriptionName_AcceptsIdsAndFullNames(string subscription, string expected) =>
        Assert.Equal(expected, GooglePubSubMapping.ToSubscriptionName(subscription, "p1").ToString());

    [Fact]
    public void ToTopicName_RejectsMalformedFullNames() =>
        Assert.ThrowsAny<ArgumentException>(() => GooglePubSubMapping.ToTopicName("projects/p1/queues/orders", "p1"));

    private static TransportMessage Outgoing(string? partitionKey) => new(
        "msg-1",
        "order-placed",
        "orders",
        "{}"u8.ToArray(),
        "application/json",
        new Dictionary<string, string>
        {
            [TransportHeaders.TraceParent] = "00-abc-def-01",
            [TransportHeaders.TenantId] = "tenant-ü",
        },
        partitionKey);
}
