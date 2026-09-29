namespace Twinbox.Transport;

public sealed record TransportMessage(
    string MessageId,
    string MessageName,
    string Destination,
    ReadOnlyMemory<byte> Body,
    string ContentType,
    IReadOnlyDictionary<string, string> Headers,
    string? PartitionKey);
