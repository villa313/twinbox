using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Twinbox.InMemory;
using Twinbox.Migration;
using Twinbox.Storage;
using Twinbox.Testing.Conformance;
using Twinbox.Transport;

namespace Twinbox.Relational.IntegrationTests;

[Trait("Category", "Integration")]
public abstract class RelationalTests<TDatabase>(TDatabase database) : IClassFixture<TDatabase>, IAsyncLifetime
    where TDatabase : Database
{
    private static readonly JsonSerializerOptions WebJson = new(JsonSerializerDefaults.Web);

    public async ValueTask InitializeAsync()
    {
        await using var services = await StartAsync();
        await using var connection = database.Connect();
        await connection.ExecuteAsync(database.CreateOrdersTable);
        await connection.ExecuteAsync("DELETE FROM orders");
        await connection.ExecuteAsync($"DELETE FROM {Table("TwinboxOutbox")}");
        await connection.ExecuteAsync($"DELETE FROM {Table("TwinboxInbox")}");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Theory]
    [MemberData(nameof(StoreCases.All), MemberType = typeof(StoreCases))]
    public async Task Store_MeetsStorageContract(ConformanceCase<IOutboxStore> conformanceCase)
    {
        await using var services = await StartAsync();

        await conformanceCase.RunAsync(services.GetServices<IOutboxStore>().Single());
    }

    [Fact]
    public async Task CommitAsync_SavesDataAndMessageInOneTransaction()
    {
        await using var services = await StartAsync();

        await using (var scope = services.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            await using var connection = database.Connect();
            await connection.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            await connection.ExecuteAsync(database.InsertOrder, new { reference = "D-1" }, transaction);
            outbox.Send(new OrderPlaced("D-1"));
            await outbox.CommitAsync(transaction);
        }

        Assert.Equal(1, await services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default));
        Assert.Equal("orders", Assert.Single(services.GetRequiredService<InMemoryTransport>().Sent).Destination);
    }

    [Fact]
    public async Task RolledBackTransaction_SavesNothing()
    {
        await using var services = await StartAsync();

        await using (var scope = services.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            await using var connection = database.Connect();
            await connection.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            await connection.ExecuteAsync(database.InsertOrder, new { reference = "D-2" }, transaction);
            outbox.Send(new OrderPlaced("D-2"));
            await outbox.SaveAsync(transaction);
            await transaction.RollbackAsync();
        }

        Assert.Equal(0, await CountAsync("orders"));
        Assert.Equal(0, await CountAsync(Table("TwinboxOutbox")));
    }

    [Fact]
    public async Task Inbox_HandlerWritesThroughItsTransactionAndRunsOnce()
    {
        await using var services = await StartAsync();
        var pipeline = services.GetRequiredService<IInboundPipeline>();
        var message = Incoming("h-1", new PlaceOrder("H-1"));

        await pipeline.ProcessAsync(message, default);
        await pipeline.ProcessAsync(message with { DeliveryAttempt = 2 }, default);

        Assert.Equal(1, await CountAsync("orders"));
        Assert.Equal(1, await CountAsync(Table("TwinboxOutbox")));
    }

    [Fact]
    public async Task Inbox_FailedHandler_RollsBackItsWritesAndMessages()
    {
        await using var services = await StartAsync();
        var pipeline = services.GetRequiredService<IInboundPipeline>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.ProcessAsync(Incoming("h-2", new PlaceOrder("H-2", Fail: true)), default));

        Assert.Equal(0, await CountAsync("orders"));
        Assert.Equal(0, await CountAsync(Table("TwinboxOutbox")));
        Assert.Equal(0, await CountAsync(Table("TwinboxInbox")));
    }

    [Fact]
    public async Task Inbox_Batch_SkipsDuplicatesAndCommitsTogether()
    {
        await using var services = await StartAsync();
        var pipeline = services.GetRequiredService<IInboundPipeline>();

        await pipeline.ProcessBatchAsync([Reserve("s-1", "R-1"), Reserve("s-2", "R-2")], default);
        await pipeline.ProcessBatchAsync([Reserve("s-2", "R-2"), Reserve("s-3", "R-3")], default);

        Assert.Equal(3, await CountAsync("orders"));
    }

    [Fact]
    public async Task Inbox_ConcurrentDuplicates_AreHandledOnce()
    {
        await using var services = await StartAsync();
        var pipeline = services.GetRequiredService<IInboundPipeline>();
        var message = Incoming("h-3", new PlaceOrder("H-3"));

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => pipeline.ProcessAsync(message, default))));

        Assert.Equal(1, await CountAsync("orders"));
    }

    [Fact]
    public async Task CompetingDispatchers_DeliverEachMessageOnce()
    {
        const int messageCount = 200;
        await using var first = await StartAsync("dispatcher-1");
        await using var second = await StartAsync("dispatcher-2");

        await using (var scope = first.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            await using var connection = database.Connect();
            await connection.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            for (var i = 0; i < messageCount; i++)
            {
                outbox.Send(new OrderPlaced($"P-{i}"));
            }

            await outbox.CommitAsync(transaction);
        }

        await Task.WhenAll(DrainAsync(first), DrainAsync(second));

        var delivered = first.GetRequiredService<InMemoryTransport>().Sent
            .Concat(second.GetRequiredService<InMemoryTransport>().Sent)
            .Select(m => m.MessageId)
            .ToArray();
        Assert.Equal(messageCount, delivered.Length);
        Assert.Equal(messageCount, delivered.Distinct().Count());
    }

    [Fact]
    public void HandlerTransaction_OutsideAHandler_Throws()
    {
        var transaction = new HandlerTransaction();

        Assert.False(transaction.IsActive);
        Assert.Throws<InvalidOperationException>(() => transaction.Connection);
    }

    [Fact]
    public async Task Import_ResumesAfterACrashBetweenAppendAndMark()
    {
        var legacy = database.Legacy;
        await using (var connection = database.Connect())
        {
            await connection.ExecuteAsync(legacy.Create);
            await connection.ExecuteAsync("DELETE FROM legacy_outbox");
            await connection.ExecuteAsync(legacy.Insert, new
            {
                id = legacy.NewId,
                name = nameof(OrderPlaced),
                content = """{"reference":"L-1"}""",
                headers = """{"x-legacy":"yes"}""",
                partitionKey = "customer-7",
            });
        }

        // The first run appends, then fails to mark the old row, as if the process died in between.
        await using (var crashed = await StartAsync(importMark: legacy.MarkImported.Replace("legacy_outbox", "legacy_missing", StringComparison.Ordinal)))
        {
            Assert.Equal(0, await crashed.GetRequiredService<OutboxImportService>().ImportBatchAsync(default));
        }

        await using var services = await StartAsync(importMark: legacy.MarkImported);
        var importer = services.GetRequiredService<OutboxImportService>();
        Assert.Equal(1, await importer.ImportBatchAsync(default));
        Assert.Equal(0, await importer.ImportBatchAsync(default));
        Assert.Equal(1, await CountAsync(Table("TwinboxOutbox")));

        await DrainAsync(services);
        var sent = Assert.Single(services.GetRequiredService<InMemoryTransport>().Sent);
        Assert.Equal("customer-7", sent.PartitionKey);
        Assert.Equal("yes", sent.Headers["x-legacy"]);
    }

    [Fact]
    public async Task Import_CarriesAttemptsLastErrorAndDeadStatus()
    {
        var legacy = database.Legacy;
        await using (var connection = database.Connect())
        {
            await connection.ExecuteAsync(legacy.Create);
            await connection.ExecuteAsync("DELETE FROM legacy_outbox");
            await connection.ExecuteAsync(legacy.Insert, new
            {
                id = legacy.NewId,
                name = nameof(OrderPlaced),
                content = """{"reference":"L-2"}""",
                headers = (string?)null,
                partitionKey = (string?)null,
            });
        }

        // The old system had given up on this row; the optional columns say so.
        var select = Regex.Replace(legacy.SelectPending, @"AS ""?PartitionKey""?", "$0, 3 AS attempts, 'gave up' AS lasterror, 1 AS dead");
        await using var services = await StartAsync(importMark: legacy.MarkImported, importSelect: select);
        Assert.Equal(1, await services.GetRequiredService<OutboxImportService>().ImportBatchAsync(default));

        var admin = services.GetServices<IOutboxStore>().OfType<IOutboxAdmin>().Single();
        var dead = Assert.Single((await admin.QueryAsync(new OutboxQuery { Status = OutboxMessageStatus.Dead }, default)).Messages);
        Assert.Equal((3, "gave up"), (dead.Attempts, dead.LastError));
        Assert.Equal(0, (await services.GetServices<IOutboxStore>().Single().GetStatisticsAsync(default)).PendingCount);
    }

    private static async Task DrainAsync(IServiceProvider services)
    {
        var dispatcher = services.GetRequiredService<IOutboxDispatcher>();
        while (await dispatcher.DispatchBatchAsync(default) > 0)
        {
        }
    }

    protected async Task<ServiceProvider> StartAsync(string? instanceId = null, string? importMark = null, string? importSelect = null)
    {
        var collection = new ServiceCollection().AddLogging().AddSingleton<Database>(database);
        collection.AddTwinbox(b =>
        {
            database.UseStore(b);
            if (importMark is not null)
            {
                b.ImportFromExistingOutbox(o =>
                {
                    o.CreateConnection = _ => database.Connect();
                    o.SelectPending = importSelect ?? database.Legacy.SelectPending;
                    o.MarkImported = importMark;
                });
            }

            b.UseInMemoryTransport(o => o.AutoDeliver = false)
                .Route<OrderPlaced>().To("orders")
                .AddHandler<PlaceOrderHandler, PlaceOrder>()
                .AddBatchHandler<ReserveStockHandler, ReserveStock>()
                .Configure(o =>
                {
                    o.Dispatcher.Enabled = false;
                    o.Dispatcher.BatchSize = 25;
                    if (instanceId is not null)
                    {
                        o.InstanceId = instanceId;
                    }
                });
        });
        var services = collection.BuildServiceProvider(validateScopes: true);

        // Only the schema startup service matters here; the rest are disabled or idle, and the import is driven by hand.
        foreach (var hosted in services.GetServices<IHostedService>().Where(h => h is not OutboxImportService))
        {
            await hosted.StartAsync(default);
        }

        return services;
    }

    private async Task<int> CountAsync(string table)
    {
        await using var connection = database.Connect();
        return await connection.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM {table}");
    }

    private string Table(string name) => database switch
    {
        SqlServerDatabase => $"[messaging].[{name}]",
        MySqlDatabase => $"messaging.`{name}`",
        _ => $"messaging.\"{name}\"",
    };

    private static IncomingMessage Reserve(string id, string reference) => new(
        id,
        nameof(ReserveStock),
        "stock",
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new ReserveStock(reference), WebJson)),
        "application/json",
        new Dictionary<string, string>(),
        1,
        null);

    private static IncomingMessage Incoming(string id, PlaceOrder message) => new(
        id,
        nameof(PlaceOrder),
        "orders",
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, WebJson)),
        "application/json",
        new Dictionary<string, string>(),
        1,
        null);

    public sealed class ReserveStockHandler(HandlerTransaction transaction, Database database) : IHandleBatch<ReserveStock>
    {
        public async Task HandleAsync(IReadOnlyList<BatchItem<ReserveStock>> batch, CancellationToken cancellationToken)
        {
            foreach (var item in batch)
            {
                await transaction.Connection.ExecuteAsync(database.InsertOrder, new { reference = item.Message.Reference }, transaction.Transaction);
            }
        }
    }

    public sealed class PlaceOrderHandler(HandlerTransaction transaction, IOutbox outbox, Database database) : IHandle<PlaceOrder>
    {
        public async Task HandleAsync(PlaceOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            await transaction.Connection.ExecuteAsync(
                database.InsertOrder,
                new { reference = message.Reference },
                transaction.Transaction);
            outbox.Send(new OrderPlaced(message.Reference));
            if (message.Fail)
            {
                throw new InvalidOperationException("handler failed");
            }
        }
    }
}

public sealed record PlaceOrder(string Reference, bool Fail = false);

public sealed record ReserveStock(string Reference);

public sealed record OrderPlaced(string Reference);

public static class StoreCases
{
    public static TheoryData<ConformanceCase<IOutboxStore>> All => [.. OutboxStoreConformance.Cases, .. OutboxAdminConformance.Cases];
}

public sealed class PostgreSqlTests(PostgreSqlDatabase database) : RelationalTests<PostgreSqlDatabase>(database);

public sealed class SqlServerTests(SqlServerDatabase database) : RelationalTests<SqlServerDatabase>(database);

public sealed class MySqlTests(MySqlDatabase database) : RelationalTests<MySqlDatabase>(database);
