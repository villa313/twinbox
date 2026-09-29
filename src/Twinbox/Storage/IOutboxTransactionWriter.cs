using System.Data.Common;

namespace Twinbox.Storage;

/// <summary>Writes outbox messages through a caller's ADO.NET transaction; provided by the relational store packages.</summary>
public interface IOutboxTransactionWriter
{
    Task WriteAsync(DbTransaction transaction, IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken);
}
