namespace Twinbox.EntityFrameworkCore;

internal sealed class InboxRecord
{
    public required string MessageId { get; init; }

    public required string Consumer { get; init; }

    public required string Source { get; init; }

    public required DateTimeOffset ProcessedAt { get; init; }
}
