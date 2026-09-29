using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace Twinbox.UnitTests;

internal sealed class TestHost : IAsyncDisposable
{
    private TestHost(ServiceProvider services, FakeTimeProvider time)
    {
        Services = services;
        Time = time;
    }

    public ServiceProvider Services { get; }

    public FakeTimeProvider Time { get; }

    public Testing.TwinboxTestHarness Harness => Services.GetTwinboxHarness();

    public static TestHost Create(Action<TwinboxBuilder> configure, Action<IServiceCollection>? services = null)
    {
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var collection = new ServiceCollection()
            .AddLogging()
            .AddSingleton<TimeProvider>(time);
        services?.Invoke(collection);
        collection.AddTwinbox(builder =>
        {
            builder.UseTestHarness();
            configure(builder);
        });
        return new TestHost(collection.BuildServiceProvider(validateScopes: true), time);
    }

    /// <summary>Sends inside a scope and commits it, like a request that saves its unit of work.</summary>
    public async Task SendAsync(Action<IOutbox> send)
    {
        await using var scope = Services.CreateAsyncScope();
        send(scope.ServiceProvider.GetRequiredService<IOutbox>());
        await scope.ServiceProvider.GetRequiredService<InMemory.InMemoryUnitOfWork>().CommitAsync();
    }

    public ValueTask DisposeAsync() => Services.DisposeAsync();
}
