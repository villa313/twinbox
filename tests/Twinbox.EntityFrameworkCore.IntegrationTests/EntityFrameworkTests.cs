using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.InMemory;
using Twinbox.Storage;
using Twinbox.Testing.Conformance;
using Twinbox.Transport;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

[Trait("Category", "Integration")]
public abstract class EntityFrameworkTests<TFixture>(TFixture database) : IClassFixture<TFixture>, IAsyncLifetime
    where TFixture : DatabaseFixture
{
    private static readonly System.Text.Json.JsonSerializerOptions WebJson = new(System.Text.Json.JsonSerializerDefaults.Web);

    public async ValueTask InitializeAsync()
    {
        await using var services = BuildServices();
        await using var scope = services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<ShopContext>();
        await database.EnsureSchemaAsync(context);
        await context.TwinboxOutbox().ExecuteDeleteAsync();
        await context.TwinboxInbox().ExecuteDeleteAsync();
        await context.Orders.ExecuteDeleteAsync();

        var billing = scope.ServiceProvider.GetRequiredService<BillingContext>();
        await database.EnsureBillingSchemaAsync(billing);
        await billing.TwinboxOutbox().ExecuteDeleteAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Theory]
    [MemberData(nameof(StoreCases.All), MemberType = typeof(StoreCases))]
    public async Task Store_MeetsStorageContract(ConformanceCase<IOutboxStore> conformanceCase)
    {
        await using var services = BuildServices();

        await conformanceCase.RunAsync(services.GetRequiredService<IOutboxStore>());
    }

    [Fact]
    public async Task SaveChanges_StoresDataAndMessageTogether()
    {
        await using var services = BuildServices();

        await using (var scope = services.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<ShopContext>();
            context.Orders.Add(new Order { Reference = "A-1" });
            scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced("A-1"));
            await context.SaveChangesAsync();
        }

        Assert.Equal(1, await services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default));
        var sent = Assert.Single(services.GetRequiredService<InMemoryTransport>().Sent);
        Assert.Equal("orders", sent.Destination);
    }

    [Fact]
    public async Task FailedSaveChanges_StoresNothing()
    {
        await using var services = BuildServices();
        await SeedOrderAsync(services, "DUP");

        await using (var scope = services.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<ShopContext>();
            context.Orders.Add(new Order { Reference = "DUP" });
            scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced("DUP"));
            await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync());
        }

        Assert.Equal(0, await CountOutboxAsync(services));
    }

    [Fact]
    public async Task RolledBackTransaction_DiscardsMessages()
    {
        await using var services = BuildServices();

        await using (var scope = services.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<ShopContext>();
            await using var transaction = await context.Database.BeginTransactionAsync();
            context.Orders.Add(new Order { Reference = "R-1" });
            scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced("R-1"));
            await context.SaveChangesAsync();
            await transaction.RollbackAsync();
        }

        Assert.Equal(0, await CountOutboxAsync(services));
    }

    [Fact]
    public async Task Dispatcher_IsWokenOnlyAfterCommit()
    {
        var signal = new CountingSignal();
        await using var services = BuildServices(s => s.AddSingleton<IDispatchSignal>(signal));
        await using var scope = services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<ShopContext>();

        await using (var transaction = await context.Database.BeginTransactionAsync())
        {
            scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced("T-1"));
            await context.SaveChangesAsync();
            Assert.Equal(0, signal.Count);

            await transaction.CommitAsync();
            Assert.Equal(1, signal.Count);
        }

        scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced("T-2"));
        await context.SaveChangesAsync();
        Assert.Equal(2, signal.Count);
    }

    [Fact]
    public async Task Inbox_RedeliveredMessage_IsHandledOnce()
    {
        await using var services = BuildServices();
        var pipeline = services.GetRequiredService<IInboundPipeline>();
        var message = Incoming("msg-1", new PlaceOrder("I-1"));

        await pipeline.ProcessAsync(message, default);
        await pipeline.ProcessAsync(message with { DeliveryAttempt = 2 }, default);

        Assert.Equal(1, await CountOrdersAsync(services));
        Assert.Equal(1, await CountOutboxAsync(services));
    }

    [Fact]
    public async Task Inbox_FailedHandler_RollsBackItsWritesAndMessages()
    {
        await using var services = BuildServices();
        var pipeline = services.GetRequiredService<IInboundPipeline>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.ProcessAsync(Incoming("msg-2", new PlaceOrder("F-1", Fail: true)), default));

        Assert.Equal(0, await CountOrdersAsync(services));
        Assert.Equal(0, await CountOutboxAsync(services));

        await pipeline.ProcessAsync(Incoming("msg-2", new PlaceOrder("F-1")), default);
        Assert.Equal(1, await CountOrdersAsync(services));
    }

    [Fact]
    public async Task Inbox_ConcurrentDuplicates_AreHandledOnce()
    {
        await using var services = BuildServices();
        var pipeline = services.GetRequiredService<IInboundPipeline>();
        var message = Incoming("msg-3", new PlaceOrder("C-1"));

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => pipeline.ProcessAsync(message, default))));

        Assert.Equal(1, await CountOrdersAsync(services));
    }

    [Fact]
    public async Task Inbox_WorksWithRetryingExecutionStrategy()
    {
        await using var services = BuildServices(retryOnFailure: true);

        await services.GetRequiredService<IInboundPipeline>().ProcessAsync(Incoming("msg-4", new PlaceOrder("E-1")), default);

        Assert.Equal(1, await CountOrdersAsync(services));
    }

    [Fact]
    public async Task CompetingDispatchers_DeliverEachMessageOnce()
    {
        const int messageCount = 200;
        await using var first = BuildServices(configure: o => o.InstanceId = "dispatcher-1");
        await using var second = BuildServices(configure: o => o.InstanceId = "dispatcher-2");

        await using (var scope = first.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            for (var i = 0; i < messageCount; i++)
            {
                outbox.Send(new OrderPlaced($"P-{i}"));
            }

            await scope.ServiceProvider.GetRequiredService<ShopContext>().SaveChangesAsync();
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
    public async Task EveryContextsOutbox_IsDispatched()
    {
        await using var services = BuildServices();

        await using (var scope = services.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            outbox.Send(new OrderPlaced("shop"));
            await scope.ServiceProvider.GetRequiredService<ShopContext>().SaveChangesAsync();
            outbox.Send(new OrderPlaced("billing"));
            await scope.ServiceProvider.GetRequiredService<BillingContext>().SaveChangesAsync();
        }

        Assert.Equal(2, await services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default));
    }

    [Fact]
    public async Task Purge_RemovesProcessedInboxEntries()
    {
        await using var services = BuildServices();
        await services.GetRequiredService<IInboundPipeline>().ProcessAsync(Incoming("msg-5", new PlaceOrder("G-1")), default);
        var inbox = services.GetRequiredService<IInboxStore>();

        var purged = await inbox.PurgeAsync(DateTimeOffset.UtcNow.AddMinutes(1), 100, default);

        Assert.Equal(1, purged);
    }

    private ServiceProvider BuildServices(
        Action<IServiceCollection>? services = null,
        bool retryOnFailure = false,
        Action<TwinboxOptions>? configure = null)
    {
        var collection = new ServiceCollection().AddLogging();
        services?.Invoke(collection);
        collection.AddDbContext<ShopContext>(o => database.Configure(o, retryOnFailure));
        collection.AddDbContext<BillingContext>(o => database.Configure(o, retryOnFailure));
        collection.AddTwinbox(b => b
            .UseEntityFrameworkCore<ShopContext>()
            .UseEntityFrameworkCore<BillingContext>()
            .UseInMemoryTransport(o => o.AutoDeliver = false)
            .Route<OrderPlaced>().To("orders")
            .AddHandler<PlaceOrderHandler, PlaceOrder>()
            .Configure(o =>
            {
                o.Dispatcher.Enabled = false;
                o.Dispatcher.BatchSize = 25;
                configure?.Invoke(o);
            }));
        return collection.BuildServiceProvider(validateScopes: true);
    }

    private static async Task DrainAsync(IServiceProvider services)
    {
        var dispatcher = services.GetRequiredService<IOutboxDispatcher>();
        while (await dispatcher.DispatchBatchAsync(default) > 0)
        {
        }
    }

    private static async Task SeedOrderAsync(IServiceProvider services, string reference)
    {
        await using var scope = services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<ShopContext>();
        context.Orders.Add(new Order { Reference = reference });
        await context.SaveChangesAsync();
    }

    private static async Task<int> CountOutboxAsync(IServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<ShopContext>().TwinboxOutbox().CountAsync();
    }

    private static async Task<int> CountOrdersAsync(IServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<ShopContext>().Orders.CountAsync();
    }

    private static IncomingMessage Incoming(string id, PlaceOrder message) => new(
        id,
        nameof(PlaceOrder),
        "orders",
        Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(message, WebJson)),
        "application/json",
        new Dictionary<string, string>(),
        1,
        null);

    public sealed class PlaceOrderHandler(ShopContext db, IOutbox outbox) : IHandle<PlaceOrder>
    {
        public Task HandleAsync(PlaceOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            db.Orders.Add(new Order { Reference = message.Reference });
            outbox.Send(new OrderPlaced(message.Reference));
            return message.Fail
                ? throw new InvalidOperationException("handler failed")
                : Task.CompletedTask;
        }
    }

    private sealed class CountingSignal : IDispatchSignal
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Notify() => Interlocked.Increment(ref _count);
    }
}

public sealed class PostgreSqlTests(PostgreSqlFixture database) : EntityFrameworkTests<PostgreSqlFixture>(database);

public sealed class SqlServerTests(SqlServerFixture database) : EntityFrameworkTests<SqlServerFixture>(database);

public static class StoreCases
{
    public static TheoryData<ConformanceCase<IOutboxStore>> All => [.. OutboxStoreConformance.Cases];
}

public sealed class SqliteTests(SqliteFixture database) : EntityFrameworkTests<SqliteFixture>(database);
