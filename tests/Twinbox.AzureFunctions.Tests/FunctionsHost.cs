using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Twinbox.InMemory;
using Twinbox.Testing;

namespace Twinbox.AzureFunctions.Tests;

/// <summary>A service provider wired the way a Functions app would be, with in-memory storage and fake time.</summary>
internal sealed class FunctionsHost : IAsyncDisposable
{
    private FunctionsHost(ServiceProvider services, FakeTimeProvider time)
    {
        Services = services;
        Time = time;
    }

    public ServiceProvider Services { get; }

    public FakeTimeProvider Time { get; }

    public IOutboxDispatcher Dispatcher => Services.GetRequiredService<IOutboxDispatcher>();

    public TwinboxTestHarness Harness => Services.GetTwinboxHarness();

    public static FunctionsHost Create(Action<TwinboxBuilder>? configure = null, Dictionary<string, string?>? settings = null)
    {
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<TimeProvider>(time)
            .AddSingleton(new Journal())
            .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection(settings ?? []).Build())
            .AddTwinbox(b =>
            {
                b.UseTestHarness().UseAzureFunctions();
                configure?.Invoke(b);
            });
        return new FunctionsHost(services.BuildServiceProvider(validateScopes: true), time);
    }

    public async Task SendAsync(IEnumerable<object> messages)
    {
        await using var scope = Services.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
        foreach (var message in messages)
        {
            outbox.Send(message);
        }

        await scope.ServiceProvider.GetRequiredService<InMemoryUnitOfWork>().CommitAsync();
    }

    public ValueTask DisposeAsync() => Services.DisposeAsync();
}
