namespace Twinbox;

public sealed record MessageContext(
    string MessageId,
    string MessageName,
    string Source,
    IReadOnlyDictionary<string, string> Headers,
    int DeliveryAttempt,
    string? PartitionKey)
{
    public string? CorrelationId { get; init; }

    public string? ReplyTo { get; init; }
}
