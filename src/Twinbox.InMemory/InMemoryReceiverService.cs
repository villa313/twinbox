using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.InMemory;

internal sealed class InMemoryReceiverService(
    InMemoryTransport transport,
    IInboundPipeline pipeline,
    IOptions<InMemoryOptions> options) : BackgroundService
{
    private static readonly TimeSpan IdleWait = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Value.AutoDeliver)
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await transport.WaitForMessagesAsync(IdleWait, stoppingToken).ConfigureAwait(false);
                await transport.DeliverAsync(pipeline, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
        }
    }
}
