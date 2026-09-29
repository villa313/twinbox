namespace Twinbox.Transport;

/// <summary>
/// Another system's header names for the message id, name and partition key. Twinbox writes them on every outgoing
/// message and reads them from incoming ones, so services can talk both ways while they move over one at a time.
/// </summary>
public sealed record HeaderProfile(string MessageId, string MessageName, string? PartitionKey = null)
{
    /// <summary>Header that receives the time the message was sent, in ISO 8601.</summary>
    public string? SentTime { get; init; }

    /// <summary>Headers written with a fixed value on every message.</summary>
    public IReadOnlyDictionary<string, string>? Constants { get; init; }

    /// <summary>
    /// CloudEvents binary mode, so non-.NET systems can consume Twinbox messages. The prefix is "ce-" for HTTP,
    /// "ce_" for Kafka and "cloudEvents:" for AMQP.
    /// </summary>
    public static HeaderProfile CloudEvents(string source, string prefix = "ce-")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        return new HeaderProfile($"{prefix}id", $"{prefix}type")
        {
            SentTime = $"{prefix}time",
            Constants = new Dictionary<string, string>
            {
                [$"{prefix}specversion"] = "1.0",
                [$"{prefix}source"] = source,
            },
        };
    }

    /// <summary>Headers named "{prefix}-msg-id" and "{prefix}-msg-name", a common scheme among outbox libraries.</summary>
    public static HeaderProfile Prefixed(string prefix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        return new HeaderProfile($"{prefix}-msg-id", $"{prefix}-msg-name");
    }
}
