namespace Twinbox;

public sealed record MessageContext(
    string MessageId,
    string MessageName,
    string Source,
    IReadOnlyDictionary<string, string> Headers,
    int DeliveryAttempt,
    string? PartitionKey);
