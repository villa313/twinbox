using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.InMemory;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

[Trait("Category", "Integration")]
public sealed class TenancyTests(PostgreSqlFixture database) : IClassFixture<PostgreSqlFixture>
{
    private static readonly string[] Tenants = ["tenant_a", "tenant_b"];

    [Fact]
    public async Task EachTenantsOutbox_IsStoredAndDispatchedFromItsOwnDatabase()
    {
        await using var services = await BuildServicesAsync();

        foreach (var tenant in Tenants)
        {
            await using var scope = services.CreateAsyncScope();
            scope.ServiceProvider.GetRequiredService<TenantContext>().Id = tenant;
            var context = scope.ServiceProvider.GetRequiredService<ShopContext>();
            context.Orders.Add(new Order { Reference = $"{tenant}-order" });
            scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(tenant));
            await context.SaveChangesAsync();
        }

        Assert.Equal(2, await services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default));

        var sent = services.GetRequiredService<InMemoryTransport>().Sent;
        Assert.Equal(Tenants, sent.Select(m => m.Headers[TransportHeaders.TenantId]).Order());
        foreach (var tenant in Tenants)
        {
            var row = Assert.Single(await OutboxRowsAsync(services, tenant));
            Assert.Equal(OutboxMessageStatus.Sent, row.Status);
            Assert.Equal(tenant, row.TenantId);
        }
    }

    [Fact]
    public async Task IncomingMessage_IsHandledInItsTenantsDatabase()
    {
        await using var services = await BuildServicesAsync();
        var body = Encoding.UTF8.GetBytes("""{"reference":"routed"}""");
        var headers = new Dictionary<string, string> { [TransportHeaders.TenantId] = "tenant_b" };

        await services.GetRequiredService<IInboundPipeline>().ProcessAsync(
            new IncomingMessage("t-1", nameof(PlaceOrder), "orders", body, "application/json", headers, 1, null),
            default);

        Assert.Equal(0, await CountOrdersAsync(services, "tenant_a", "routed"));
        Assert.Equal(1, await CountOrdersAsync(services, "tenant_b", "routed"));
    }

    private async Task<ServiceProvider> BuildServicesAsync()
    {
        var collection = new ServiceCollection().AddLogging().AddScoped<TenantContext>();
        collection.AddDbContext<ShopContext>((sp, o) =>
            o.UseNpgsql(database.ConnectionStringFor(sp.GetRequiredService<TenantContext>().Id ?? "postgres")));
        collection.AddTwinbox(b => b
            .UseEntityFrameworkCore<ShopContext>()
            .UseInMemoryTransport(o => o.AutoDeliver = false)
            .Route<OrderPlaced>().To("orders")
            .AddHandler<EntityFrameworkTests<PostgreSqlFixture>.PlaceOrderHandler, PlaceOrder>()
            .UseTenants(o =>
            {
                o.ListTenants = (_, _) => Task.FromResult<IReadOnlyCollection<string>>(Tenants);
                o.EnterTenant = (sp, tenant) => sp.GetRequiredService<TenantContext>().Id = tenant;
                o.CurrentTenant = sp => sp.GetRequiredService<TenantContext>().Id;
            })
            .Configure(o => o.Dispatcher.Enabled = false));
        var services = collection.BuildServiceProvider(validateScopes: true);

        foreach (var tenant in Tenants)
        {
            await using var scope = services.CreateAsyncScope();
            scope.ServiceProvider.GetRequiredService<TenantContext>().Id = tenant;
            var context = scope.ServiceProvider.GetRequiredService<ShopContext>();
            await context.Database.EnsureCreatedAsync();
            await context.Set<OutboxMessage>().ExecuteDeleteAsync();
            await context.Orders.ExecuteDeleteAsync();
        }

        return services;
    }

    private static async Task<List<OutboxMessage>> OutboxRowsAsync(IServiceProvider services, string tenant)
    {
        await using var scope = services.CreateAsyncScope();
        scope.ServiceProvider.GetRequiredService<TenantContext>().Id = tenant;
        return await scope.ServiceProvider.GetRequiredService<ShopContext>().Set<OutboxMessage>().AsNoTracking().ToListAsync();
    }

    private static async Task<int> CountOrdersAsync(IServiceProvider services, string tenant, string reference)
    {
        await using var scope = services.CreateAsyncScope();
        scope.ServiceProvider.GetRequiredService<TenantContext>().Id = tenant;
        return await scope.ServiceProvider.GetRequiredService<ShopContext>().Orders.CountAsync(o => o.Reference == reference);
    }

    public sealed class TenantContext
    {
        public string? Id { get; set; }
    }
}
