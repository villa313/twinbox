using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

public sealed class EnlistmentTests : IAsyncLifetime
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"twinbox-enlist-{Guid.NewGuid():N}.db");

    private string ConnectionString => $"Data Source={_path}";

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        File.Delete(_path);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task PooledContext_SavesTheScopesMessages()
    {
        await using var services = await BuildAsync(s => s.AddDbContextPool<ShopContext>(o => o.UseSqlite(ConnectionString)));

        await SendAndSaveAsync(services, "first");
        await SendAndSaveAsync(services, "second");

        Assert.Equal(2, await CountOutboxAsync(services));
    }

    [Fact]
    public async Task ContextCreatedByHand_SavesMessagesOnceEnlisted()
    {
        await using var services = await BuildAsync(s => s.AddDbContext<ShopContext>(o => o.UseSqlite(ConnectionString)));
        var options = new DbContextOptionsBuilder<ShopContext>().UseSqlite(ConnectionString).Options;

        await using (var scope = services.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            await using var context = new ShopContext(options).EnlistOutbox(outbox);
            context.Orders.Add(new Order { Reference = "manual" });
            outbox.Send(new OrderPlaced("manual"));
            await context.SaveChangesAsync();
        }

        Assert.Equal(1, await CountOutboxAsync(services));
    }

    [Fact]
    public void UseEntityFrameworkCore_BeforeAddDbContext_ExplainsTheOrder()
    {
        var services = new ServiceCollection();

        var error = Assert.Throws<InvalidOperationException>(() => services.AddTwinbox(b => b.UseEntityFrameworkCore<ShopContext>()));

        Assert.Contains("before UseEntityFrameworkCore", error.Message, StringComparison.Ordinal);
    }

    private static async Task<ServiceProvider> BuildAsync(Action<IServiceCollection> addContext)
    {
        var collection = new ServiceCollection().AddLogging();
        addContext(collection);
        collection.AddTwinbox(b => b
            .UseEntityFrameworkCore<ShopContext>()
            .UseInMemoryTransport(o => o.AutoDeliver = false)
            .Route<OrderPlaced>().To("orders")
            .Configure(o => o.Dispatcher.Enabled = false));
        var services = collection.BuildServiceProvider(validateScopes: true);

        await using var scope = services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<ShopContext>().Database.EnsureCreatedAsync();
        return services;
    }

    private static async Task SendAndSaveAsync(IServiceProvider services, string reference)
    {
        await using var scope = services.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<ShopContext>();
        context.Orders.Add(new Order { Reference = reference });
        scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(reference));
        await context.SaveChangesAsync();
    }

    private static async Task<int> CountOutboxAsync(IServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<ShopContext>().TwinboxOutbox().CountAsync();
    }
}
