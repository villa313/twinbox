using Azure.Messaging.EventHubs.Processor;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.InMemory;
using Twinbox.Transport;

namespace Twinbox.EventHubs.Tests.Integration;

internal sealed class EventHubsTestHost : IAsyncDisposable
{
    private static readonly TimeSpan StartupTimeout = TimeSpan.FromSeconds(60);

    private readonly IHost _host;

    private EventHubsTestHost(IHost host)
    {
        _host = host;
    }

    public IServiceProvider Services => _host.Services;

    public ITransport Transport => Services.GetServices<ITransport>().Single(t => t.Name == EventHubsTransport.TransportName);

    public InMemoryOutboxStore Outbox => Services.GetRequiredService<InMemoryOutboxStore>();

    public BatchLog BatchLog => Services.GetRequiredService<BatchLog>();

    public static async Task<EventHubsTestHost> StartAsync(
        EventHubsFixture emulator,
        Journal journal,
        FailureGate gate,
        Action<TwinboxBuilder> configure,
        Action<EventHubsOptions>? eventHubs = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();

        // The emulator is sometimes slow to close links; a short timeout keeps shutdown from stalling the run.
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(10));
        builder.Services.AddSingleton(journal);
        builder.Services.AddSingleton(gate);
        builder.Services.AddSingleton<BatchLog>();
        builder.Services.AddTwinbox(twinbox =>
        {
            twinbox
                .UseInMemoryStore()
                .UseEventHubs(options =>
                {
                    options.ConnectionString = emulator.ConnectionString;
                    options.CreateCheckpointContainers = true;
                    options.RetryDelay = TimeSpan.FromMilliseconds(100);
                    options.MaxRetryDelay = TimeSpan.FromMilliseconds(500);

                    // One processor per test: claim every partition at once instead of one per balancing cycle.
                    options.ConfigureProcessor = o =>
                    {
                        o.LoadBalancingStrategy = LoadBalancingStrategy.Greedy;
                        o.LoadBalancingUpdateInterval = TimeSpan.FromSeconds(1);
                    };
                    eventHubs?.Invoke(options);
                })
                .Configure(options => options.Dispatcher.MinPollInterval = TimeSpan.FromMilliseconds(100));
            configure(twinbox);
        });

        var host = builder.Build();
        await host.StartAsync();
        var processors = host.Services.GetServices<IHostedService>().OfType<EventHubsProcessorService>().Single();
        await processors.Ready.WaitAsync(StartupTimeout);
        return new EventHubsTestHost(host);
    }

    /// <summary>Sends inside a scope and commits it, like a request that saves its unit of work.</summary>
    public async Task SendAsync<TMessage>(TMessage message)
        where TMessage : class
    {
        await using var scope = Services.CreateAsyncScope();
        scope.ServiceProvider.GetRequiredService<IOutbox>().Send(message);
        await scope.ServiceProvider.GetRequiredService<InMemoryUnitOfWork>().CommitAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }
}
