using System.Data.Common;
#if (AdoSqlServer)
using Microsoft.Data.SqlClient;
#elif (AdoPostgres)
using Npgsql;
#else
using MySqlConnector;
#endif

namespace TwinboxApp.Data;

public sealed class OrderDatabase(string connectionString)
{
#if (AdoSqlServer)
    private const string CreateOrdersTable =
        "IF OBJECT_ID(N'orders') IS NULL CREATE TABLE orders (id uniqueidentifier PRIMARY KEY, total decimal(18, 2) NOT NULL)";
#elif (AdoPostgres)
    private const string CreateOrdersTable =
        "CREATE TABLE IF NOT EXISTS orders (id uuid PRIMARY KEY, total numeric(18, 2) NOT NULL)";
#else
    private const string CreateOrdersTable =
        "CREATE TABLE IF NOT EXISTS orders (id char(36) PRIMARY KEY, total decimal(18, 2) NOT NULL)";
#endif

    public async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
#if (AdoSqlServer)
        var connection = new SqlConnection(connectionString);
#elif (AdoPostgres)
        var connection = new NpgsqlConnection(connectionString);
#else
        var connection = new MySqlConnection(connectionString);
#endif
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = CreateOrdersTable;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public static async Task InsertAsync(DbTransaction transaction, Order order, CancellationToken cancellationToken)
    {
        await using var command = transaction.Connection!.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO orders (id, total) VALUES (@id, @total)";
        AddParameter(command, "@id", order.Id);
        AddParameter(command, "@total", order.Total);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
