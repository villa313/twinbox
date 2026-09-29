namespace Twinbox.Storage;

public sealed record OutboxMessage
{
    public required Guid Id { get; init; }

    public required string MessageName { get; init; }

    public required string Transport { get; init; }

    public required string Destination { get; init; }

    public string? PartitionKey { get; init; }

    public string? TenantId { get; init; }

    public required byte[] Payload { get; init; }

    public required string ContentType { get; init; }

    public IReadOnlyDictionary<string, string> Headers { get; init; } = EmptyHeaders.Instance;

    /// <summary>W3C trace context captured at send time, so delivery joins the originating trace.</summary>
    public string? TraceParent { get; init; }

    public required DateTimeOffset CreatedAt { get; init; }

    public required DateTimeOffset AvailableAt { get; init; }

    public int Attempts { get; init; }

    public OutboxMessageStatus Status { get; init; }

    public string? LeaseOwner { get; init; }

    public DateTimeOffset? LeaseUntil { get; init; }

    public string? LastError { get; init; }

    public DateTimeOffset? SentAt { get; init; }
}
