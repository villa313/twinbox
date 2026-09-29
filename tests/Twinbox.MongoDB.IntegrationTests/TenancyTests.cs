using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MongoDB.Bson;
using MongoDB.Driver;
using Twinbox.InMemory;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.MongoDB.IntegrationTests;

[Trait("Category", "Integration")]
public sealed class TenancyTests(MongoFixture mongo) : IAsyncLifetime
{
    private readonly string _prefix = $"t{Guid.NewGuid():N}"[..12];

    private string[] Tenants => [$"{_prefix}_a", $"{_prefix}_b"];

    public async ValueTask InitializeAsync()
    {
        foreach (var tenant in Tenants)
        {
            await mongo.Client.GetDatabase(tenant).CreateCollectionAsync("orders");
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var tenant in Tenants)
        {
            await mongo.Client.DropDatabaseAsync(tenant);
        }
    }

    [Fact]
    public async Task EachTenantsOutbox_IsStoredAndDispatchedFromItsOwnDatabase()
    {
        await using var services = await StartAsync();

        foreach (var tenant in Tenants)
        {
            await using var scope = services.CreateAsyncScope();
            scope.ServiceProvider.GetRequiredService<TenantContext>().Id = tenant;
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            using var session = await mongo.Client.StartSessionAsync();
            session.StartTransaction();
            outbox.Send(new OrderPlaced(tenant));
            await outbox.CommitAsync(session);
        }

        Assert.Equal(2, await services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default));

        var sent = services.GetRequiredService<InMemoryTransport>().Sent;
        Assert.Equal(Tenants, sent.Select(m => m.Headers[TransportHeaders.TenantId]).Order());
        foreach (var tenant in Tenants)
        {
            var document = Assert.Single(await mongo.Collection(tenant, "twinbox_outbox").Find(FilterDefinition<BsonDocument>.Empty).ToListAsync());
            Assert.Equal((int)OutboxMessageStatus.Sent, document["Status"].AsInt32);
            Assert.Equal(tenant, document["TenantId"].AsString);
        }
    }

    [Fact]
    public async Task IncomingMessage_IsHandledInItsTenantsDatabase()
    {
        await using var services = await StartAsync();
        var body = Encoding.UTF8.GetBytes("""{"reference":"routed"}""");
        var headers = new Dictionary<string, string> { [TransportHeaders.TenantId] = Tenants[1] };

        await services.GetRequiredService<IInboundPipeline>().ProcessAsync(
            new IncomingMessage("t-1", nameof(PlaceOrder), "orders", body, "application/json", headers, 1, null),
            default);

        Assert.Equal(0, await mongo.CountAsync(Tenants[0], "orders"));
        Assert.Equal(1, await mongo.CountAsync(Tenants[1], "orders"));
    }

    private async Task<ServiceProvider> StartAsync()
    {
        var tenants = Tenants;
        var collection = new ServiceCollection().AddLogging().AddScoped<TenantContext>();
        collection.AddTwinbox(b => b
            .UseMongoDB(o =>
            {
                o.ConnectionString = mongo.ConnectionString;
                o.DatabaseNameFactory = sp => sp.GetRequiredService<TenantContext>().Id ?? "admin";
            })
            .UseInMemoryTransport(o => o.AutoDeliver = false)
            .Route<OrderPlaced>().To("orders")
            .AddHandler<MongoTests.PlaceOrderHandler, PlaceOrder>()
            .UseTenants(o =>
            {
                o.ListTenants = (_, _) => Task.FromResult<IReadOnlyCollection<string>>(tenants);
                o.EnterTenant = (sp, tenant) => sp.GetRequiredService<TenantContext>().Id = tenant;
                o.CurrentTenant = sp => sp.GetRequiredService<TenantContext>().Id;
            })
            .Configure(o => o.Dispatcher.Enabled = false));
        var provider = collection.BuildServiceProvider(validateScopes: true);
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StartAsync(default);
        }

        return provider;
    }

    public sealed class TenantContext
    {
        public string? Id { get; set; }
    }
}
