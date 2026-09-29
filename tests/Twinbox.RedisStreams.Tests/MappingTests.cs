using System.Text;
using StackExchange.Redis;
using Twinbox.Transport;

namespace Twinbox.RedisStreams.Tests;

public sealed class MappingTests
{
    [Fact]
    public void ToEntry_WritesIdentityContentTypeAndBody()
    {
        var fields = Fields(RedisStreamsMapping.ToEntry(Outgoing(partitionKey: null)));

        Assert.Equal(
            [RedisStreamsMapping.IdField, RedisStreamsMapping.NameField, RedisStreamsMapping.ContentTypeField, RedisStreamsMapping.BodyField, RedisStreamsMapping.HeadersField],
            fields.Keys);
        Assert.Equal("msg-1", (string?)fields[RedisStreamsMapping.IdField]);
        Assert.Equal("order-placed", (string?)fields[RedisStreamsMapping.NameField]);
        Assert.Equal("application/json", (string?)fields[RedisStreamsMapping.ContentTypeField]);
        Assert.Equal("{}", Encoding.UTF8.GetString((byte[])fields[RedisStreamsMapping.BodyField]!));
    }

    [Fact]
    public void ToEntry_EncodesHeadersAsJsonWithThePartitionKey()
    {
        var fields = Fields(RedisStreamsMapping.ToEntry(Outgoing(partitionKey: "customer-7")));

        var headers = Serialization.HeaderCodec.Decode(fields[RedisStreamsMapping.HeadersField]);
        Assert.Equal("00-abc-def-01", headers[TransportHeaders.TraceParent]);
        Assert.Equal("tenant-ü", headers[TransportHeaders.TenantId]);
        Assert.Equal("customer-7", headers[TransportHeaders.PartitionKey]);
    }

    [Fact]
    public void ToEntry_DropsHeadersThatDuplicateFieldsAndStalePartitionKeys()
    {
        var outgoing = Outgoing(partitionKey: null) with
        {
            Headers = new Dictionary<string, string>
            {
                [TransportHeaders.MessageId] = "stale",
                [TransportHeaders.MessageName] = "stale",
                [TransportHeaders.PartitionKey] = "stale",
            },
        };

        var fields = Fields(RedisStreamsMapping.ToEntry(outgoing));

        Assert.Equal(string.Empty, (string?)fields[RedisStreamsMapping.HeadersField]);
        Assert.Equal("msg-1", (string?)fields[RedisStreamsMapping.IdField]);
    }

    [Fact]
    public void ToIncomingMessage_RoundTripsAnOutgoingMessage()
    {
        var entry = new StreamEntry("1-0", RedisStreamsMapping.ToEntry(Outgoing(partitionKey: "customer-7")));

        var incoming = RedisStreamsMapping.ToIncomingMessage("orders", entry, deliveryAttempt: 3);

        Assert.Equal("msg-1", incoming.MessageId);
        Assert.Equal("order-placed", incoming.MessageName);
        Assert.Equal("orders", incoming.Source);
        Assert.Equal("application/json", incoming.ContentType);
        Assert.Equal("customer-7", incoming.PartitionKey);
        Assert.Equal("{}", Encoding.UTF8.GetString(incoming.Body.Span));
        Assert.Equal("00-abc-def-01", incoming.Headers[TransportHeaders.TraceParent]);
        Assert.Equal("tenant-ü", incoming.Headers[TransportHeaders.TenantId]);
        Assert.Equal(3, incoming.DeliveryAttempt);
    }

    [Fact]
    public void ToIncomingMessage_ForeignEntry_FallsBackToStableCoordinates()
    {
        var entry = new StreamEntry("1700000000000-4", [new NameValueEntry("payload", "x")]);

        var first = RedisStreamsMapping.ToIncomingMessage("orders", entry);
        var redelivered = RedisStreamsMapping.ToIncomingMessage("orders", entry, deliveryAttempt: 2);

        Assert.Equal("orders:1700000000000-4", first.MessageId);
        Assert.Equal(first.MessageId, redelivered.MessageId);
        Assert.Equal(string.Empty, first.MessageName);
        Assert.Equal("application/octet-stream", first.ContentType);
        Assert.True(first.Body.IsEmpty);
        Assert.Empty(first.Headers);
        Assert.Null(first.PartitionKey);
    }

    [Fact]
    public void ToIncomingMessage_EmptyIdField_FallsBackToCoordinates()
    {
        var entry = new StreamEntry("5-0", [new NameValueEntry(RedisStreamsMapping.IdField, string.Empty)]);

        Assert.Equal("orders:5-0", RedisStreamsMapping.ToIncomingMessage("orders", entry).MessageId);
    }

    [Fact]
    public void ToIncomingMessage_UnreadableHeaders_ArePermanent()
    {
        var entry = new StreamEntry("5-0", [new NameValueEntry(RedisStreamsMapping.HeadersField, "{not json")]);

        var error = Assert.Throws<PermanentDeliveryException>(() => RedisStreamsMapping.ToIncomingMessage("orders", entry));

        Assert.Contains("5-0", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ToDeadLetter_KeepsTheEntryAndAddsFailureDetails()
    {
        var entry = new StreamEntry("9-1", RedisStreamsMapping.ToEntry(Outgoing(partitionKey: "customer-7")));

        var fields = Fields(RedisStreamsMapping.ToDeadLetter("orders", "billing", entry, "PermanentDeliveryException: bad payload"));

        Assert.Equal("msg-1", (string?)fields[RedisStreamsMapping.IdField]);
        Assert.Equal("order-placed", (string?)fields[RedisStreamsMapping.NameField]);
        Assert.Equal("{}", Encoding.UTF8.GetString((byte[])fields[RedisStreamsMapping.BodyField]!));
        Assert.Equal("PermanentDeliveryException: bad payload", (string?)fields[RedisStreamsMapping.ErrorField]);
        Assert.Equal("orders:9-1", (string?)fields[RedisStreamsMapping.OriginField]);
        Assert.Equal("billing", (string?)fields[RedisStreamsMapping.GroupField]);
    }

    [Fact]
    public void ToDeadLetter_PinsTheFallbackIdAndReplacesEarlierFailureFields()
    {
        var entry = new StreamEntry("4-0",
        [
            new NameValueEntry(RedisStreamsMapping.ErrorField, "old"),
            new NameValueEntry(RedisStreamsMapping.OriginField, "elsewhere:1-0"),
            new NameValueEntry(RedisStreamsMapping.GroupField, "other"),
        ]);

        var dead = RedisStreamsMapping.ToDeadLetter("orders", "billing", entry, "new");

        Assert.Single(dead, f => f.Name == RedisStreamsMapping.ErrorField);
        var fields = Fields(dead);
        Assert.Equal("orders:4-0", (string?)fields[RedisStreamsMapping.IdField]);
        Assert.Equal("new", (string?)fields[RedisStreamsMapping.ErrorField]);
        Assert.Equal("orders:4-0", (string?)fields[RedisStreamsMapping.OriginField]);
        Assert.Equal("billing", (string?)fields[RedisStreamsMapping.GroupField]);
    }

    [Fact]
    public void ToDeadLetter_TruncatesLongErrors()
    {
        var dead = Fields(RedisStreamsMapping.ToDeadLetter("orders", "billing", new StreamEntry("1-0", []), new string('x', 5000)));

        Assert.Equal(2000, ((string?)dead[RedisStreamsMapping.ErrorField])!.Length);
    }

    [Fact]
    public void DeadStream_IsSuffixedWithDead() => Assert.Equal("orders:dead", RedisStreamsMapping.DeadStream("orders"));

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

    private static Dictionary<string, RedisValue> Fields(NameValueEntry[] entries) =>
        entries.ToDictionary(e => (string)e.Name!, e => e.Value);
}
