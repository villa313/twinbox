using System.Data.Common;

namespace Twinbox;

/// <summary>
/// The connection and transaction a handler must use with plain ADO.NET or Dapper, so its writes commit together with
/// the inbox entry and any messages it sends. Only active while a relational inbox is running the handler.
/// </summary>
public sealed class HandlerTransaction
{
    private DbConnection? _connection;
    private DbTransaction? _transaction;

    public bool IsActive => _transaction is not null;

    public DbConnection Connection => _connection ?? throw NotActive();

    public DbTransaction Transaction => _transaction ?? throw NotActive();

    /// <summary>Called by store packages around a handler run.</summary>
    public void Attach(DbConnection connection, DbTransaction transaction)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
    }

    public void Detach()
    {
        _connection = null;
        _transaction = null;
    }

    private static InvalidOperationException NotActive() => new(
        "No handler transaction is active. HandlerTransaction is only available inside a handler run by the SQL Server or PostgreSQL inbox.");
}
