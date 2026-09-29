namespace Twinbox.Transport;

public sealed record IncomingMessage(
    string MessageId,
    string MessageName,
    string Source,
    ReadOnlyMemory<byte> Body,
    string ContentType,
    IReadOnlyDictionary<string, string> Headers,
    int DeliveryAttempt,
    string? PartitionKey);
