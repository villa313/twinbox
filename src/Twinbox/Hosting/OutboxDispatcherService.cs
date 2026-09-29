using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Dispatch;

namespace Twinbox.Hosting;

internal sealed partial class OutboxDispatcherService(
    IOutboxDispatcher dispatcher,
    DispatchSignal signal,
    IOptions<TwinboxOptions> options,
    ILogger<OutboxDispatcherService> logger) : BackgroundService
{
    private readonly ILogger _logger = logger;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value.Dispatcher;
        if (!settings.Enabled)
        {
            return;
        }

        var idleDelay = settings.MinPollInterval;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var claimed = await dispatcher.DispatchBatchAsync(stoppingToken).ConfigureAwait(false);
                if (claimed >= settings.BatchSize)
                {
                    continue;
                }

                idleDelay = claimed > 0 ? settings.MinPollInterval : Backoff(idleDelay, settings.MaxPollInterval);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                LogDispatchFailed(ex);
                idleDelay = settings.MaxPollInterval;
            }

            try
            {
                if (await signal.WaitAsync(idleDelay, stoppingToken).ConfigureAwait(false))
                {
                    idleDelay = settings.MinPollInterval;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private static TimeSpan Backoff(TimeSpan current, TimeSpan max) =>
        TimeSpan.FromTicks(Math.Min(current.Ticks * 2, max.Ticks));

    [LoggerMessage(Level = LogLevel.Error, Message = "Outbox dispatch pass failed; backing off.")]
    private partial void LogDispatchFailed(Exception error);
}
