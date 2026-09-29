using System.Data.Common;
using Twinbox.Storage;

namespace Twinbox.Relational;

internal sealed class RelationalTransactionWriter(RelationalDialect dialect) : IOutboxTransactionWriter
{
    public Task WriteAsync(DbTransaction transaction, IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(messages);
        return RelationalOutboxStore.InsertAsync(dialect, transaction, messages, cancellationToken);
    }
}
