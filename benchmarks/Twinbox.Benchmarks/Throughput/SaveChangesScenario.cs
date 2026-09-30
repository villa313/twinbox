using System.Data.Common;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Twinbox.Benchmarks.Throughput;

/// <summary>Transactions per second, each writing one order row plus N outbox messages; N = 0 is the no-outbox baseline.</summary>
internal static class SaveChangesScenario
{
    public static async Task<Measurement> RunAsync(BenchStore store, int messagesPerUnit, int workers, int units, int runs)
    {
        await using var services = store.Build();
        return await Measurement.TakeAsync(runs, store.ResetAsync, async () =>
        {
            var remaining = units;
            var stopwatch = Stopwatch.StartNew();
            await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
            {
                while (Interlocked.Decrement(ref remaining) >= 0)
                {
                    await (store.IsAdo
                        ? CommitAdoAsync(services, store.Database, messagesPerUnit)
                        : SaveEntityFrameworkAsync(services, messagesPerUnit));
                }
            })));
            return units / stopwatch.Elapsed.TotalSeconds;
        });
    }

    private static async Task SaveEntityFrameworkAsync(IServiceProvider services, int messages)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<BenchContext>();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
        db.Orders.Add(new Order { Reference = "order" });
        for (var i = 0; i < messages; i++)
        {
            outbox.Send(SampleMessages.Small());
        }

        await db.SaveChangesAsync();
    }

    private static async Task CommitAdoAsync(IServiceProvider services, BenchDatabase database, int messages)
    {
        await using var scope = services.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
        await using var connection = database.Connect();
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await InsertOrderAsync(connection, transaction, BenchDatabase.InsertAdoOrder);
        for (var i = 0; i < messages; i++)
        {
            outbox.Send(SampleMessages.Small());
        }

        await outbox.CommitAsync(transaction);
    }

    private static async Task InsertOrderAsync(DbConnection connection, DbTransaction transaction, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        var reference = command.CreateParameter();
        reference.ParameterName = "@reference";
        reference.Value = "order";
        command.Parameters.Add(reference);
        await command.ExecuteNonQueryAsync();
    }
}
