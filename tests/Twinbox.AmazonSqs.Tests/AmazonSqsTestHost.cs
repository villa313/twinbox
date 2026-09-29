using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.InMemory;
using Twinbox.Transport;

namespace Twinbox.AmazonSqs.Tests;

internal sealed class AmazonSqsTestHost : IAsyncDisposable
{
    private static readonly TimeSpan StartupTimeout = TimeSpan.FromSeconds(60);

    private readonly IHost _host;

    private AmazonSqsTestHost(IHost host)
    {
        _host = host;
    }

    public IServiceProvider Services => _host.Services;

    public ITransport Transport => Services.GetServices<ITransport>().Single(t => t.Name == AmazonSqsTransport.TransportName);

    public InMemoryOutboxStore Outbox => Services.GetRequiredService<InMemoryOutboxStore>();

    public static async Task<AmazonSqsTestHost> StartAsync(
        LocalStackFixture localStack,
        Journal journal,
        FailureGate gate,
        Action<TwinboxBuilder> configure,
        Action<AmazonSqsOptions>? sqs = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(journal);
        builder.Services.AddSingleton(gate);
        builder.Services.AddTwinbox(twinbox =>
        {
            twinbox
                .UseInMemoryStore()
                .UseAmazonSqs(options =>
                {
                    options.ServiceUrl = localStack.ServiceUrl;
                    options.Region = LocalStackFixture.Region;
                    options.Credentials = LocalStackFixture.Credentials;
                    options.AutoCreate = true;
                    options.WaitTimeSeconds = 1;
                    sqs?.Invoke(options);
                })
                .Configure(options => options.Dispatcher.MinPollInterval = TimeSpan.FromMilliseconds(100));
            configure(twinbox);
        });

        var host = builder.Build();
        await host.StartAsync();
        var receiver = host.Services.GetServices<IHostedService>().OfType<AmazonSqsReceiverService>().Single();
        await receiver.Ready.WaitAsync(StartupTimeout);
        return new AmazonSqsTestHost(host);
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
