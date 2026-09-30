using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Twinbox.InMemory;
using Twinbox.Migration;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

public sealed class MigrationTests : IAsyncLifetime
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"twinbox-migrate-{Guid.NewGuid():N}.db");

    private string ConnectionString => $"Data Source={_path}";

    public async ValueTask InitializeAsync()
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE legacy_published (Id INTEGER PRIMARY KEY, Name TEXT, Content TEXT, Status TEXT, Properties TEXT, OrderingKey TEXT);
            INSERT INTO legacy_published VALUES (7195478239, 'shop.order.placed', '{"Reference":"L-1"}', 'Scheduled', '{"legacy-corr-id":"c-1"}', 'order-1');
            INSERT INTO legacy_published VALUES (7195478240, 'shop.order.placed', '{"Reference":"L-2"}', 'Succeeded', NULL, NULL);
            INSERT INTO legacy_published VALUES (7195478241, 'something.unknown', '{}', 'Scheduled', NULL, NULL);
            CREATE TABLE legacy_received (Id INTEGER PRIMARY KEY, GroupName TEXT, Status TEXT);
            INSERT INTO legacy_received VALUES (900, 'shipping', 'Succeeded');
            """;
        await command.ExecuteNonQueryAsync();
    }

    public ValueTask DisposeAsync()
    {
        SqliteConnection.ClearAllPools();
        File.Delete(_path);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Import_MovesPendingRowsIntoTheOutboxOnce()
    {
        await using var services = await BuildAsync();
        var importer = services.GetRequiredService<OutboxImportService>();

        Assert.Equal(1, await importer.ImportBatchAsync(default));
        Assert.Equal(0, await importer.ImportBatchAsync(default));

        await services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default);
        var sent = Assert.Single(services.GetRequiredService<InMemoryTransport>().Sent);
        Assert.Equal("shop.order.placed", sent.MessageName);
        Assert.Equal("""{"Reference":"L-1"}""", System.Text.Encoding.UTF8.GetString(sent.Body.Span));
        Assert.Equal("order-1", sent.PartitionKey);
        Assert.Equal("c-1", sent.Headers["legacy-corr-id"]);
    }

    [Fact]
    public async Task Import_WithSeveralStores_NeedsAnExplicitStore()
    {
        await using var services = await BuildAsync(b => b.UseInMemoryStore());

        var error = Assert.Throws<InvalidOperationException>(() => services.GetRequiredService<OutboxImportService>());

        Assert.Contains("ShopContext, InMemory", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Import_WritesToTheChosenStore()
    {
        await using var services = await BuildAsync(b => b.UseInMemoryStore(), store: "InMemory");

        Assert.Equal(1, await services.GetRequiredService<OutboxImportService>().ImportBatchAsync(default));

        Assert.Single(services.GetRequiredService<InMemoryOutboxStore>().Snapshot());
    }

    [Fact]
    public async Task ImportAndSeed_RunOncePerTenant()
    {
        var entered = new System.Collections.Concurrent.ConcurrentBag<string>();
        await using var services = await BuildAsync(
            b => b.UseInMemoryStore().UseTenants(o =>
            {
                o.ListTenants = (_, _) => Task.FromResult<IReadOnlyCollection<string>>(["acme", "globex"]);
                o.EnterTenant = (_, tenant) => entered.Add(tenant);
                o.CurrentTenant = _ => null;
            }),
            store: "InMemory");

        // Both tenants read the same legacy table here, so the second finds its row already marked.
        Assert.Equal(1, await services.GetRequiredService<OutboxImportService>().ImportBatchAsync(default));
        Assert.Equal("acme", Assert.Single(services.GetRequiredService<InMemoryOutboxStore>().Snapshot()).TenantId);
        Assert.Contains("acme", entered);
        Assert.Contains("globex", entered);
    }

    [Fact]
    public async Task Seed_MarksProcessedMessagesSoTheyAreSkipped()
    {
        await using var services = await BuildAsync();
        foreach (var hosted in services.GetServices<IHostedService>().OfType<IHostedService>().Where(h => h.GetType().Name == "InboxSeedService"))
        {
            await hosted.StartAsync(default);
        }

        await using var scope = services.CreateAsyncScope();
        var processed = await services.GetRequiredService<IInboxStore>().TryProcessAsync(
            new InboxEntry("900", "shipping", "orders", DateTimeOffset.UtcNow),
            scope.ServiceProvider,
            _ => Task.CompletedTask,
            default);

        Assert.False(processed);
    }

    [Fact]
    public void ImportedIds_AreDeterministicPerDestination()
    {
        Assert.Equal(ImportedMessageIds.For("1", "orders"), ImportedMessageIds.For("1", "orders"));
        Assert.NotEqual(ImportedMessageIds.For("1", "orders"), ImportedMessageIds.For("1", "billing"));
    }

    private async Task<ServiceProvider> BuildAsync(Action<TwinboxBuilder>? configure = null, string? store = null)
    {
        var collection = new ServiceCollection().AddLogging();
        collection.AddDbContext<ShopContext>(o => o.UseSqlite(ConnectionString));
        collection.AddTwinbox(b => b
            .UseEntityFrameworkCore<ShopContext>()
            .UseInMemoryTransport(o => o.AutoDeliver = false)
            .Route<LegacyOrderPlaced>().To("orders")
            .ImportFromExistingOutbox(o =>
            {
                o.CreateConnection = _ => new SqliteConnection(ConnectionString);
                o.SelectPending = """
                    SELECT Id, Name, Content, Properties AS Headers, OrderingKey AS PartitionKey
                    FROM legacy_published WHERE Status = 'Scheduled' ORDER BY Id LIMIT @batch
                    """;
                o.MarkImported = "UPDATE legacy_published SET Status = 'Migrated' WHERE Id = @id";
                o.Store = store;
            })
            .SeedInboxFromExisting(o =>
            {
                o.CreateConnection = _ => new SqliteConnection(ConnectionString);
                o.SelectProcessed = "SELECT Id AS MessageId, GroupName AS Consumer FROM legacy_received WHERE Status = 'Succeeded'";
            })
            .Configure(o => o.Dispatcher.Enabled = false)
            .Apply(configure));
        var services = collection.BuildServiceProvider(validateScopes: true);

        await using var scope = services.CreateAsyncScope();
        // EnsureCreated skips databases that already have tables, like this one with the old system's.
        await scope.ServiceProvider.GetRequiredService<ShopContext>()
            .GetService<Microsoft.EntityFrameworkCore.Storage.IRelationalDatabaseCreator>()
            .CreateTablesAsync();
        return services;
    }

    [MessageName("shop.order.placed")]
    public sealed record LegacyOrderPlaced(string Reference);
}

internal static class TwinboxBuilderTestExtensions
{
    public static TwinboxBuilder Apply(this TwinboxBuilder builder, Action<TwinboxBuilder>? configure)
    {
        configure?.Invoke(builder);
        return builder;
    }
}
