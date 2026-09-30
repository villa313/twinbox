namespace Twinbox.Storage;

/// <summary>Optional store capability behind the dashboard and tooling: browse, replay and remove outbox messages.</summary>
public interface IOutboxAdmin
{
    /// <summary>Newest first; pass <see cref="OutboxPage.NextCursor"/> back as <see cref="OutboxQuery.Cursor"/> for the next page.</summary>
    Task<OutboxPage> QueryAsync(OutboxQuery query, CancellationToken cancellationToken);

    Task<OutboxMessage?> GetAsync(Guid id, CancellationToken cancellationToken);

    /// <summary>Puts messages back to pending with a fresh attempt count, due at <paramref name="now"/>. Returns how many changed.</summary>
    Task<int> ReplayAsync(IReadOnlyCollection<Guid> ids, DateTimeOffset now, CancellationToken cancellationToken);

    Task<int> DeleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken);
}

public sealed record OutboxQuery
{
    public OutboxMessageStatus? Status { get; init; }

    public string? Destination { get; init; }

    public string? MessageName { get; init; }

    /// <summary>Matches a message id or partition key exactly.</summary>
    public string? Search { get; init; }

    public int Take { get; init; } = 50;

    public string? Cursor { get; init; }
}

public sealed record OutboxPage(IReadOnlyList<OutboxMessage> Messages, string? NextCursor);
