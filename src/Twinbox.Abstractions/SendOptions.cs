namespace Twinbox;

public sealed record SendOptions
{
    /// <summary>Messages sharing a key are delivered in order; unkeyed messages are delivered in parallel.</summary>
    public string? PartitionKey { get; init; }

    public TimeSpan? Delay { get; init; }

    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>Sends to this logical destination instead of the message type's routes; the destination prefix applies.</summary>
    public string? Destination { get; init; }

    /// <summary>Transport for <see cref="Destination"/>; only needed when several transports are registered.</summary>
    public string? Transport { get; init; }

    /// <summary>Where the receiver should reply: a physical address, sent as is without the destination prefix.</summary>
    public string? ReplyTo { get; init; }

    /// <summary>Defaults to the correlation id of the message being handled, so a whole conversation shares one.</summary>
    public string? CorrelationId { get; init; }
}
