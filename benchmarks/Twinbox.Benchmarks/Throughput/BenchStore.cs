using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Benchmarks.Throughput;

/// <summary>One database reached through either the EF Core store or the plain ADO.NET store.</summary>
internal sealed class BenchStore(BenchDatabase database, bool ado)
{
    public BenchDatabase Database => database;

    public bool IsAdo => ado;

    public string Name => $"{database.Name}, {(ado ? "ADO.NET" : "EF Core")}";

    public ServiceProvider Build(Action<TwinboxBuilder>? configure = null)
    {
        var services = new ServiceCollection().AddLogging();
        services.AddSingleton(database);
        services.AddSingleton<ITransport, NoOpTransport>();
        if (!ado)
        {
            services.AddDbContext<BenchContext>(database.ConfigureEntityFramework);
        }

        services.AddTwinbox(twinbox =>
        {
            twinbox.Route<OrderPlaced>().To("orders");
            if (ado)
            {
                database.UseAdoStore(twinbox);
                twinbox.AddHandler<AdoOrderHandler, OrderPlaced>();
            }
            else
            {
                twinbox.UseEntityFrameworkCore<BenchContext>();
                twinbox.AddHandler<EntityFrameworkOrderHandler, OrderPlaced>();
            }

            twinbox.Configure(o => o.Retention.Enabled = false);
            configure?.Invoke(twinbox);
        });
        return services.BuildServiceProvider();
    }

    /// <summary>Creates the tables if needed and empties them.</summary>
    public async Task ResetAsync()
    {
        await using var services = Build();
        if (!ado)
        {
            await using var scope = services.CreateAsyncScope();
            var context = scope.ServiceProvider.GetRequiredService<BenchContext>();
            await context.CreateTablesAsync();
            await context.ClearAsync(database.Clear);
            return;
        }

        // Any store call creates the ADO.NET store's schema on first use.
        await services.GetRequiredService<IOutboxStore>().GetStatisticsAsync(default);
        await using var connection = database.Connect();
        await connection.OpenAsync();
        foreach (var statement in new[]
        {
            database.CreateAdoOrders,
            database.Clear("ado_orders"),
            database.Clear(database.AdoTable("TwinboxOutbox")),
            database.Clear(database.AdoTable("TwinboxInbox")),
        })
        {
            await Execute(connection, statement);
        }
    }

    /// <summary>Refreshes planner statistics after a bulk load, as a long-running database would have them.</summary>
    public async Task AnalyzeAsync()
    {
        if (database.Analyze is not { } analyze)
        {
            return;
        }

        await using var connection = database.Connect();
        await connection.OpenAsync();
        await Execute(connection, analyze);
    }

    private static async Task Execute(DbConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
#pragma warning disable CA2100 // Fixed statements built from constants.
        command.CommandText = sql;
#pragma warning restore CA2100
        await command.ExecuteNonQueryAsync();
    }
}

internal sealed class EntityFrameworkOrderHandler(BenchContext db) : IHandle<OrderPlaced>
{
    public async Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        db.Orders.Add(new Order { Reference = context.MessageId });
        await db.SaveChangesAsync(cancellationToken);
    }
}

internal sealed class AdoOrderHandler(HandlerTransaction transaction, BenchDatabase database) : IHandle<OrderPlaced>
{
    public async Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        if (transaction.IsActive)
        {
            await InsertAsync(transaction.Connection, transaction.Transaction, context.MessageId, cancellationToken);
            return;
        }

        await using var connection = database.Connect();
        await connection.OpenAsync(cancellationToken);
        await InsertAsync(connection, null, context.MessageId, cancellationToken);
    }

    private async Task InsertAsync(DbConnection connection, DbTransaction? tx, string reference, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = BenchDatabase.InsertAdoOrder;
        command.Transaction = tx;
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@reference";
        parameter.Value = reference;
        command.Parameters.Add(parameter);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
