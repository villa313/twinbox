using System.ComponentModel;
using System.Data.Common;

namespace Twinbox.Storage;

/// <summary>For store authors: exposes an ADO.NET inbox's connection and transaction to the handler it runs.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class HandlerTransactionBinding
{
    /// <summary>Makes <paramref name="handlerTransaction"/> active until the result is disposed, which the store does once the handler returns.</summary>
    public static IDisposable Bind(HandlerTransaction handlerTransaction, DbConnection connection, DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(handlerTransaction);
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        handlerTransaction.Attach(connection, transaction);
        return new Binding(handlerTransaction);
    }

    private sealed class Binding(HandlerTransaction handlerTransaction) : IDisposable
    {
        public void Dispose() => handlerTransaction.Detach();
    }
}
