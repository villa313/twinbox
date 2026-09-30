using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.InMemory;
using Twinbox.Transport;

namespace Twinbox.GooglePubSub.Tests;

internal sealed class GooglePubSubTestHost : IAsyncDisposable
{
    private static readonly TimeSpan StartupTimeout = TimeSpan.FromSeconds(60);

    private readonly IHost _host;

    private GooglePubSubTestHost(IHost host)
    {
        _host = host;
    }

    public IServiceProvider Services => _host.Services;

    public ITransport Transport => Services.GetServices<ITransport>().Single(t => t.Name == GooglePubSubTransport.TransportName);

    public InMemoryOutboxStore Outbox => Services.GetRequiredService<InMemoryOutboxStore>();

    public static async Task<GooglePubSubTestHost> StartAsync(
        PubSubFixture pubSub,
        Journal journal,
        FailureGate gate,
        Action<TwinboxBuilder> configure,
        Action<GooglePubSubOptions>? options = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(journal);
        builder.Services.AddSingleton(gate);
        builder.Services.AddTwinbox(twinbox =>
        {
            twinbox
                .UseInMemoryStore()
                .UseGooglePubSub(o =>
                {
                    o.ProjectId = PubSubFixture.ProjectId;
                    o.EmulatorHost = pubSub.EmulatorHost;
                    o.AutoCreate = true;
                    options?.Invoke(o);
                })
                .Configure(o => o.Dispatcher.MinPollInterval = TimeSpan.FromMilliseconds(100));
            configure(twinbox);
        });

        var host = builder.Build();
        await host.StartAsync();
        var subscriber = host.Services.GetServices<IHostedService>().OfType<GooglePubSubSubscriberService>().Single();
        await subscriber.Ready.WaitAsync(StartupTimeout);
        return new GooglePubSubTestHost(host);
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
