using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoDB.Bson;
using MongoDB.Driver;
using Twinbox.InMemory;
using Twinbox.Storage;
using Twinbox.Testing.Conformance;
using Twinbox.Transport;

namespace Twinbox.MongoDB.IntegrationTests;

[Trait("Category", "Integration")]
public sealed class MongoTests(MongoFixture mongo) : IAsyncLifetime
{
    private const string Outbox = "twinbox_outbox";
    private const string Inbox = "twinbox_inbox";
    private const string Orders = "orders";

    private static readonly JsonSerializerOptions WebJson = new(JsonSerializerDefaults.Web);

    // A database per test gives every case the fresh, empty store the conformance suite expects.
    private readonly string _database = $"twinbox_{Guid.NewGuid():N}";

    public async ValueTask InitializeAsync() =>
        await mongo.Client.GetDatabase(_database).CreateCollectionAsync(Orders);

    public async ValueTask DisposeAsync() => await mongo.Client.DropDatabaseAsync(_database);

    [Theory]
    [MemberData(nameof(StoreCases.All), MemberType = typeof(StoreCases))]
    public async Task Store_MeetsStorageContract(ConformanceCase<IOutboxStore> conformanceCase)
    {
        await using var services = await StartAsync();

        await conformanceCase.RunAsync(services.GetServices<IOutboxStore>().Single());
    }

    [Fact]
    public async Task CommitAsync_SavesDocumentAndMessageInOneTransaction()
    {
        await using var services = await StartAsync();

        await using (var scope = services.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            using var session = await mongo.Client.StartSessionAsync();
            session.StartTransaction();
            await mongo.Collection(_database, Orders).InsertOneAsync(session, new BsonDocument("_id", "D-1"));
            outbox.Send(new OrderPlaced("D-1"));
            await outbox.CommitAsync(session);
        }

        Assert.Equal(1, await mongo.CountAsync(_database, Orders));
        Assert.Equal(1, await services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default));
        Assert.Equal("orders", Assert.Single(services.GetRequiredService<InMemoryTransport>().Sent).Destination);
    }

    [Fact]
    public async Task AbortedTransaction_SavesNothing()
    {
        await using var services = await StartAsync();

        await using (var scope = services.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            using var session = await mongo.Client.StartSessionAsync();
            session.StartTransaction();
            await mongo.Collection(_database, Orders).InsertOneAsync(session, new BsonDocument("_id", "D-2"));
            outbox.Send(new OrderPlaced("D-2"));
            await outbox.SaveAsync(session);
            await session.AbortTransactionAsync();
        }

        Assert.Equal(0, await mongo.CountAsync(_database, Orders));
        Assert.Equal(0, await mongo.CountAsync(_database, Outbox));
    }

    [Fact]
    public async Task Dispatcher_IsWokenOnlyAfterCommit()
    {
        var signal = new CountingSignal();
        await using var services = await StartAsync(s => s.AddSingleton<IDispatchSignal>(signal));
        await using var scope = services.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
        using var session = await mongo.Client.StartSessionAsync();
        session.StartTransaction();
        outbox.Send(new OrderPlaced("T-1"));

        await outbox.SaveAsync(session);
        Assert.Equal(0, signal.Count);

        await outbox.CommitAsync(session);
        Assert.Equal(1, signal.Count);
    }

    [Fact]
    public async Task Inbox_HandlerWritesThroughItsSessionAndRunsOnce()
    {
        await using var services = await StartAsync();
        var pipeline = services.GetRequiredService<IInboundPipeline>();
        var message = Incoming("h-1", new PlaceOrder("H-1"));

        await pipeline.ProcessAsync(message, default);
        await pipeline.ProcessAsync(message with { DeliveryAttempt = 2 }, default);

        Assert.Equal(1, await mongo.CountAsync(_database, Orders));
        Assert.Equal(1, await mongo.CountAsync(_database, Outbox));
        Assert.Equal(1, await mongo.CountAsync(_database, Inbox));
    }

    [Fact]
    public async Task Inbox_FailedHandler_RollsBackItsWritesAndMessages()
    {
        await using var services = await StartAsync();
        var pipeline = services.GetRequiredService<IInboundPipeline>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.ProcessAsync(Incoming("h-2", new PlaceOrder("H-2", Fail: true)), default));

        Assert.Equal(0, await mongo.CountAsync(_database, Orders));
        Assert.Equal(0, await mongo.CountAsync(_database, Outbox));
        Assert.Equal(0, await mongo.CountAsync(_database, Inbox));

        await pipeline.ProcessAsync(Incoming("h-2", new PlaceOrder("H-2")), default);
        Assert.Equal(1, await mongo.CountAsync(_database, Orders));
    }

    [Fact]
    public async Task Inbox_ConcurrentDuplicates_AreHandledOnce()
    {
        await using var services = await StartAsync();
        var pipeline = services.GetRequiredService<IInboundPipeline>();
        var message = Incoming("h-3", new PlaceOrder("H-3"));

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => pipeline.ProcessAsync(message, default))));

        Assert.Equal(1, await mongo.CountAsync(_database, Orders));
        Assert.Equal(1, await mongo.CountAsync(_database, Outbox));
    }

    [Fact]
    public async Task Inbox_Purge_RemovesProcessedEntries()
    {
        await using var services = await StartAsync();
        await services.GetRequiredService<IInboundPipeline>().ProcessAsync(Incoming("h-4", new PlaceOrder("H-4")), default);

        var purged = await services.GetRequiredService<IInboxStore>().PurgeAsync(DateTimeOffset.UtcNow.AddMinutes(1), 100, default);

        Assert.Equal(1, purged);
        Assert.Equal(0, await mongo.CountAsync(_database, Inbox));
    }

    [Fact]
    public async Task CompetingDispatchers_DeliverEachMessageOnce()
    {
        const int messageCount = 200;
        await using var first = await StartAsync(configure: o => o.InstanceId = "dispatcher-1");
        await using var second = await StartAsync(configure: o => o.InstanceId = "dispatcher-2");

        await using (var scope = first.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            for (var i = 0; i < messageCount; i++)
            {
                outbox.Send(new OrderPlaced($"P-{i}"));
            }

            using var session = await mongo.Client.StartSessionAsync();
            session.StartTransaction();
            await outbox.CommitAsync(session);
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
    public void HandlerSession_OutsideAHandler_Throws()
    {
        var session = new MongoHandlerSession();

        Assert.False(session.IsActive);
        Assert.Throws<InvalidOperationException>(() => session.Session);
    }

    private async Task<ServiceProvider> StartAsync(Action<IServiceCollection>? services = null, Action<TwinboxOptions>? configure = null)
    {
        var collection = new ServiceCollection().AddLogging();
        services?.Invoke(collection);
        collection.AddTwinbox(b => b
            .UseMongoDB(mongo.ConnectionString, _database)
            .UseInMemoryTransport(o => o.AutoDeliver = false)
            .Route<OrderPlaced>().To("orders")
            .AddHandler<PlaceOrderHandler, PlaceOrder>()
            .Configure(o =>
            {
                o.Dispatcher.Enabled = false;
                o.Dispatcher.BatchSize = 25;
                configure?.Invoke(o);
            }));
        var provider = collection.BuildServiceProvider(validateScopes: true);

        // Only the index startup service matters here; the rest are disabled or idle.
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StartAsync(default);
        }

        return provider;
    }

    private static async Task DrainAsync(IServiceProvider services)
    {
        var dispatcher = services.GetRequiredService<IOutboxDispatcher>();
        while (await dispatcher.DispatchBatchAsync(default) > 0)
        {
        }
    }

    private static IncomingMessage Incoming(string id, PlaceOrder message) => new(
        id,
        nameof(PlaceOrder),
        "orders",
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, WebJson)),
        "application/json",
        new Dictionary<string, string>(),
        1,
        null);

    public sealed class PlaceOrderHandler(MongoHandlerSession mongo, IOutbox outbox) : IHandle<PlaceOrder>
    {
        public async Task HandleAsync(PlaceOrder message, MessageContext context, CancellationToken cancellationToken)
        {
            await mongo.Database.GetCollection<BsonDocument>(Orders)
                .InsertOneAsync(mongo.Session, new BsonDocument("_id", message.Reference), cancellationToken: cancellationToken);
            outbox.Send(new OrderPlaced(message.Reference));
            if (message.Fail)
            {
                throw new InvalidOperationException("handler failed");
            }
        }
    }

    private sealed class CountingSignal : IDispatchSignal
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Notify() => Interlocked.Increment(ref _count);
    }
}

public sealed record PlaceOrder(string Reference, bool Fail = false);

public sealed record OrderPlaced(string Reference);

public static class StoreCases
{
    public static TheoryData<ConformanceCase<IOutboxStore>> All => [.. OutboxStoreConformance.Cases, .. OutboxAdminConformance.Cases];
}
