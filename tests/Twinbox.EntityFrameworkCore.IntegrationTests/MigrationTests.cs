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
            CREATE TABLE legacy_published (Id INTEGER PRIMARY KEY, Name TEXT, Content TEXT, Status TEXT);
            INSERT INTO legacy_published VALUES (7195478239, 'shop.order.placed', '{"Reference":"L-1"}', 'Scheduled');
            INSERT INTO legacy_published VALUES (7195478240, 'shop.order.placed', '{"Reference":"L-2"}', 'Succeeded');
            INSERT INTO legacy_published VALUES (7195478241, 'something.unknown', '{}', 'Scheduled');
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

    private async Task<ServiceProvider> BuildAsync()
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
                o.SelectPending = "SELECT Id, Name, Content FROM legacy_published WHERE Status = 'Scheduled' ORDER BY Id LIMIT @batch";
                o.MarkImported = "UPDATE legacy_published SET Status = 'Migrated' WHERE Id = CAST(@id AS INTEGER)";
            })
            .SeedInboxFromExisting(o =>
            {
                o.CreateConnection = _ => new SqliteConnection(ConnectionString);
                o.SelectProcessed = "SELECT Id AS MessageId, GroupName AS Consumer FROM legacy_received WHERE Status = 'Succeeded'";
            })
            .Configure(o => o.Dispatcher.Enabled = false));
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
