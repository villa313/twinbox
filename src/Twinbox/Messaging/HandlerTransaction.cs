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

    internal void Attach(DbConnection connection, DbTransaction transaction)
    {
        _connection = connection;
        _transaction = transaction;
    }

    internal void Detach()
    {
        _connection = null;
        _transaction = null;
    }

    private static InvalidOperationException NotActive() => new(
        "No handler transaction is active. HandlerTransaction is only available inside a handler run by a relational (ADO.NET) inbox; MongoDB handlers use MongoDBHandlerSession.");
}
