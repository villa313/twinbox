using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.InMemory;
using Twinbox.Transport;

namespace Twinbox.RabbitMQ.Tests;

internal sealed class RabbitMQTestHost : IAsyncDisposable
{
    private static readonly TimeSpan StartupTimeout = TimeSpan.FromSeconds(30);

    private readonly IHost _host;

    private RabbitMQTestHost(IHost host)
    {
        _host = host;
    }

    public IServiceProvider Services => _host.Services;

    public ITransport Transport => Services.GetServices<ITransport>().Single(t => t.Name == RabbitMQTransport.TransportName);

    public InMemoryOutboxStore Outbox => Services.GetRequiredService<InMemoryOutboxStore>();

    public static async Task<RabbitMQTestHost> StartAsync(
        Uri connectionUri,
        Journal journal,
        Action<TwinboxBuilder> configure,
        Action<RabbitMQOptions>? rabbit = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(journal);
        builder.Services.AddTwinbox(twinbox =>
        {
            twinbox
                .UseInMemoryStore()
                .UseRabbitMQ(options =>
                {
                    options.ConnectionUri = connectionUri;
                    options.ClientProvidedName = "twinbox-tests";
                    rabbit?.Invoke(options);
                })
                .Configure(options => options.Dispatcher.MinPollInterval = TimeSpan.FromMilliseconds(100));
            configure(twinbox);
        });

        var host = builder.Build();
        await host.StartAsync();
        var consumers = host.Services.GetServices<IHostedService>().OfType<RabbitMQConsumerService>().Single();
        await consumers.Ready.WaitAsync(StartupTimeout);
        return new RabbitMQTestHost(host);
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
