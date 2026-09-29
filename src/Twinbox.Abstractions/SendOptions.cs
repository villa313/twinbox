namespace Twinbox;

public sealed record SendOptions
{
    /// <summary>Messages sharing a key are delivered in order; unkeyed messages are delivered in parallel.</summary>
    public string? PartitionKey { get; init; }

    public TimeSpan? Delay { get; init; }

    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
