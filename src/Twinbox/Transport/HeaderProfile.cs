namespace Twinbox.Transport;

/// <summary>
/// Another system's header names for the message id, name and partition key. Twinbox writes them on every outgoing
/// message and reads them from incoming ones, so services can talk both ways while they move over one at a time.
/// </summary>
public sealed record HeaderProfile(string MessageId, string MessageName, string? PartitionKey = null)
{
    /// <summary>Headers named "{prefix}-msg-id" and "{prefix}-msg-name", a common scheme among outbox libraries.</summary>
    public static HeaderProfile Prefixed(string prefix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        return new HeaderProfile($"{prefix}-msg-id", $"{prefix}-msg-name");
    }
}
