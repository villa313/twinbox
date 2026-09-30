using System.Data.Common;
using Twinbox.Serialization;
using Twinbox.Storage;

namespace Twinbox.Relational;

internal static class OutboxRowReader
{
    public static async Task<IReadOnlyList<OutboxMessage>> ReadAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        var rows = await ReadWithSequenceAsync(reader, cancellationToken).ConfigureAwait(false);

        // RETURNING/OUTPUT order is unspecified, so restore insertion order from the key.
        return [.. rows.OrderBy(r => r.Sequence).Select(r => r.Message)];
    }

    /// <summary>Rows in the order the statement returned them.</summary>
    public static async Task<List<(long Sequence, OutboxMessage Message)>> ReadWithSequenceAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        var rows = new List<(long Sequence, OutboxMessage Message)>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add((reader.GetInt64(reader.GetOrdinal("Sequence")), Read(reader)));
        }

        return rows;
    }

    private static OutboxMessage Read(DbDataReader reader) => new()
    {
        Id = reader.GetGuid(reader.GetOrdinal("Id")),
        MessageName = reader.GetString(reader.GetOrdinal("MessageName")),
        Transport = reader.GetString(reader.GetOrdinal("Transport")),
        Destination = reader.GetString(reader.GetOrdinal("Destination")),
        PartitionKey = NullableString(reader, "PartitionKey"),
        TenantId = NullableString(reader, "TenantId"),
        Payload = reader.GetFieldValue<byte[]>(reader.GetOrdinal("Payload")),
        ContentType = reader.GetString(reader.GetOrdinal("ContentType")),
        Headers = HeaderCodec.Decode(reader.GetString(reader.GetOrdinal("Headers"))),
        TraceParent = NullableString(reader, "TraceParent"),
        CreatedAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("CreatedAt")),
        AvailableAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("AvailableAt")),
        Attempts = reader.GetInt32(reader.GetOrdinal("Attempts")),
        Status = (OutboxMessageStatus)reader.GetInt32(reader.GetOrdinal("Status")),
        LeaseOwner = NullableString(reader, "LeaseOwner"),
        LeaseUntil = NullableTimestamp(reader, "LeaseUntil"),
        LastError = NullableString(reader, "LastError"),
        SentAt = NullableTimestamp(reader, "SentAt"),
    };

    private static string? NullableString(DbDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static DateTimeOffset? NullableTimestamp(DbDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<DateTimeOffset>(ordinal);
    }
}
