using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Twinbox.InMemory;
using Twinbox.Storage;

namespace Twinbox.UnitTests;

public sealed class SetupCheckTests
{
    [Fact]
    public async Task Start_WithoutStore_NamesStorePackagesAndMethods()
    {
        var error = await StartFailsAsync(b => b.UseInMemoryTransport().Route<OrderPlaced>().To("orders"));

        Assert.StartsWith("No Twinbox outbox store is registered.", error.Message, StringComparison.Ordinal);
        Assert.All(
            ["Twinbox.EntityFrameworkCore (UseEntityFrameworkCore<TContext>())", "Twinbox.PostgreSql (UsePostgreSql(...))", "Twinbox.SqlServer (UseSqlServer(...))", "Twinbox.MongoDB (UseMongoDB(...))", "UseInMemoryStore()"],
            fix => Assert.Contains(fix, error.Message, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Start_WithoutTransport_NamesTransportPackagesAndMethods()
    {
        var error = await StartFailsAsync(b => b.UseInMemoryStore().Route<OrderPlaced>().To("orders"));

        Assert.StartsWith("No Twinbox transport is registered.", error.Message, StringComparison.Ordinal);
        Assert.All(
            ["Twinbox.RabbitMQ (UseRabbitMQ(...))", "Twinbox.AzureServiceBus (UseAzureServiceBus(...))", "Twinbox.Kafka (UseKafka(...))", "UseLocalDelivery()", "UseInMemoryTransport()"],
            fix => Assert.Contains(fix, error.Message, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Start_WithoutStoreOrTransport_ListsBothProblems()
    {
        var error = await StartFailsAsync(b => b.Route<OrderPlaced>().To("orders"));

        Assert.StartsWith("Twinbox isn't set up correctly:", error.Message, StringComparison.Ordinal);
        Assert.Contains("No Twinbox outbox store is registered.", error.Message, StringComparison.Ordinal);
        Assert.Contains("No Twinbox transport is registered.", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Start_RouteToTransportFromMissingPackage_NamesThePackage()
    {
        var error = await StartFailsAsync(b => b.UseInMemory().Route<OrderPlaced>().To("orders", transport: "rabbitmq"));

        Assert.Equal(
            "Route<OrderPlaced>().To(\"orders\", transport: \"rabbitmq\") can't be delivered. "
            + "No transport named 'rabbitmq' is registered (registered: inmemory). Install Twinbox.RabbitMQ and call UseRabbitMQ(...).",
            error.Message);
    }

    [Fact]
    public async Task Start_RouteToMisspelledTransport_SaysToCheckTheName()
    {
        var error = await StartFailsAsync(b => b.UseInMemory().Route<OrderPlaced>().To("orders", transport: "rabitmq"));

        Assert.Contains("No transport named 'rabitmq' is registered (registered: inmemory).", error.Message, StringComparison.Ordinal);
        Assert.Contains("Check the transport name in the route", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Start_SeedInboxWithoutInboxStore_NamesStorePackages()
    {
        var error = await StartFailsAsync(b => b
            .UseInMemoryTransport()
            .SeedInboxFromExisting(o =>
            {
                o.CreateConnection = _ => throw new NotSupportedException();
                o.SelectProcessed = "SELECT 1";
            }),
            services => services.AddSingleton<IOutboxStore, InMemoryOutboxStore>());

        Assert.StartsWith("SeedInboxFromExisting needs an inbox store, but none is registered.", error.Message, StringComparison.Ordinal);
        Assert.Contains("UseEntityFrameworkCore<TContext>()", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Start_ConfiguredStoreAndTransport_Starts()
    {
        using var host = Build(b => b.UseInMemory().Route<OrderPlaced>().To("orders"));

        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Start_WriteOnlyHostWithNamedTransports_Starts()
    {
        using var host = Build(b => b
            .UseInMemoryStore()
            .Configure(o => o.Dispatcher.Enabled = false)
            .Route<OrderPlaced>().To("orders", transport: "rabbitmq"));

        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Send_WithoutTransport_NamesTransportPackages()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().Route<OrderPlaced>().To("orders"));
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();

        var error = Assert.Throws<InvalidOperationException>(
            () => scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(1)));

        Assert.StartsWith("No Twinbox transport is registered.", error.Message, StringComparison.Ordinal);
        Assert.Contains("Twinbox.RabbitMQ (UseRabbitMQ(...))", error.Message, StringComparison.Ordinal);
    }

    private static async Task<InvalidOperationException> StartFailsAsync(
        Action<TwinboxBuilder> configure,
        Action<IServiceCollection>? services = null)
    {
        using var host = Build(configure, services);
        return await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(TestContext.Current.CancellationToken));
    }

    private static IHost Build(Action<TwinboxBuilder> configure, Action<IServiceCollection>? services = null)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddLogging();
        services?.Invoke(builder.Services);
        builder.Services.AddTwinbox(configure);
        return builder.Build();
    }
}
