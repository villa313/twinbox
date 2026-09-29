using MongoDB.Bson;
using Twinbox.Storage;

namespace Twinbox.MongoDB;

/// <summary>Maps outbox messages to BSON by hand, so no class maps or reflection-based serializers are involved.</summary>
internal static class OutboxDocument
{
    public const string Id = "_id";
    public const string Sequence = "Sequence";
    public const string PartitionKey = "PartitionKey";
    public const string CreatedAt = "CreatedAt";
    public const string AvailableAt = "AvailableAt";
    public const string Attempts = "Attempts";
    public const string Status = "Status";
    public const string LeaseOwner = "LeaseOwner";
    public const string LeaseUntil = "LeaseUntil";
    public const string LastError = "LastError";
    public const string SentAt = "SentAt";

    public static BsonDocument From(OutboxMessage m, long sequence) => new()
    {
        { Id, Key(m.Id) },
        { Sequence, sequence },
        { "MessageName", m.MessageName },
        { "Transport", m.Transport },
        { "Destination", m.Destination },
        { PartitionKey, Nullable(m.PartitionKey) },
        { "TenantId", Nullable(m.TenantId) },
        { "Payload", new BsonBinaryData(m.Payload) },
        { "ContentType", m.ContentType },
        // Name/value pairs rather than a sub-document, since header names may contain '.' or start with '$'.
        { "Headers", new BsonArray(m.Headers.Select(h => new BsonDocument { { "Name", h.Key }, { "Value", h.Value } })) },
        { "TraceParent", Nullable(m.TraceParent) },
        { CreatedAt, Timestamp(m.CreatedAt) },
        { AvailableAt, Timestamp(m.AvailableAt) },
        { Attempts, m.Attempts },
        { Status, (int)m.Status },
        { LeaseOwner, Nullable(m.LeaseOwner) },
        { LeaseUntil, Timestamp(m.LeaseUntil) },
        { LastError, Nullable(m.LastError) },
        { SentAt, Timestamp(m.SentAt) },
    };

    public static OutboxMessage ToMessage(BsonDocument d) => new()
    {
        Id = d[Id].AsBsonBinaryData.ToGuid(GuidRepresentation.Standard),
        MessageName = d["MessageName"].AsString,
        Transport = d["Transport"].AsString,
        Destination = d["Destination"].AsString,
        PartitionKey = NullableString(d, PartitionKey),
        TenantId = NullableString(d, "TenantId"),
        Payload = d["Payload"].AsByteArray,
        ContentType = d["ContentType"].AsString,
        Headers = ReadHeaders(d),
        TraceParent = NullableString(d, "TraceParent"),
        CreatedAt = ReadTimestamp(d[CreatedAt]),
        AvailableAt = ReadTimestamp(d[AvailableAt]),
        Attempts = d[Attempts].ToInt32(),
        Status = (OutboxMessageStatus)d[Status].ToInt32(),
        LeaseOwner = NullableString(d, LeaseOwner),
        LeaseUntil = NullableTimestamp(d, LeaseUntil),
        LastError = NullableString(d, LastError),
        SentAt = NullableTimestamp(d, SentAt),
    };

    public static BsonBinaryData Key(Guid id) => new(id, GuidRepresentation.Standard);

    public static BsonValue Timestamp(DateTimeOffset? value) =>
        value is { } v ? new BsonDateTime(v.UtcDateTime) : BsonNull.Value;

    public static DateTimeOffset ReadTimestamp(BsonValue value) => new(value.ToUniversalTime(), TimeSpan.Zero);

    private static BsonValue Nullable(string? value) => value is null ? BsonNull.Value : new BsonString(value);

    private static string? NullableString(BsonDocument d, string name) =>
        d.GetValue(name, BsonNull.Value) is { IsBsonNull: false } v ? v.AsString : null;

    private static DateTimeOffset? NullableTimestamp(BsonDocument d, string name) =>
        d.GetValue(name, BsonNull.Value) is { IsBsonNull: false } v ? ReadTimestamp(v) : null;

    private static Dictionary<string, string> ReadHeaders(BsonDocument d)
    {
        var headers = d.GetValue("Headers", BsonNull.Value);
        if (headers is not BsonArray { Count: > 0 } pairs)
        {
            return new Dictionary<string, string>();
        }

        return pairs.Select(p => p.AsBsonDocument).ToDictionary(p => p["Name"].AsString, p => p["Value"].AsString, StringComparer.Ordinal);
    }
}
