using System.Text;
using System.Text.Json;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Twinbox.InMemory;
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
            await connection.ExecuteAsync("INSERT INTO orders (reference) VALUES (@reference)", new { reference = "D-1" }, transaction);
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
            await connection.ExecuteAsync("INSERT INTO orders (reference) VALUES (@reference)", new { reference = "D-2" }, transaction);
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
    public async Task Inbox_ConcurrentDuplicates_AreHandledOnce()
    {
        await using var services = await StartAsync();
        var pipeline = services.GetRequiredService<IInboundPipeline>();
        var message = Incoming("h-3", new PlaceOrder("H-3"));

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => pipeline.ProcessAsync(message, default))));

        Assert.Equal(1, await CountAsync("orders"));
    }

    [Fact]
    public void HandlerTransaction_OutsideAHandler_Throws()
    {
        var transaction = new HandlerTransaction();

        Assert.False(transaction.IsActive);
        Assert.Throws<InvalidOperationException>(() => transaction.Connection);
    }

    private async Task<ServiceProvider> StartAsync()
    {
        var collection = new ServiceCollection().AddLogging();
        collection.AddTwinbox(b =>
        {
            database.UseStore(b);
            b.UseInMemoryTransport(o => o.AutoDeliver = false)
                .Route<OrderPlaced>().To("orders")
                .AddHandler<PlaceOrderHandler, PlaceOrder>()
                .Configure(o => o.Dispatcher.Enabled = false);
        });
        var services = collection.BuildServiceProvider(validateScopes: true);

        // Only the schema startup service matters here; the rest are disabled or idle.
        foreach (var hosted in services.GetServices<IHostedService>())
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

    private string Table(string name) => database is SqlServerDatabase ? $"[messaging].[{name}]" : $"messaging.\"{name}\"";

    private static IncomingMessage Incoming(string id, PlaceOrder message) => new(
        id,
        nameof(PlaceOrder),
        "orders",
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, WebJson)),
        "application/json",
        new Dictionary<string, string>(),
        1,
        null);

    public sealed class PlaceOrderHandler(HandlerTransaction transaction, IOutbox outbox) : IHandle<PlaceOrder>
    {
        public async Task HandleAsync(PlaceOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            await transaction.Connection.ExecuteAsync(
                "INSERT INTO orders (reference) VALUES (@reference)",
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

public sealed record OrderPlaced(string Reference);

public static class StoreCases
{
    public static TheoryData<ConformanceCase<IOutboxStore>> All => [.. OutboxStoreConformance.Cases];
}

public sealed class PostgreSqlTests(PostgreSqlDatabase database) : RelationalTests<PostgreSqlDatabase>(database);

public sealed class SqlServerTests(SqlServerDatabase database) : RelationalTests<SqlServerDatabase>(database);
