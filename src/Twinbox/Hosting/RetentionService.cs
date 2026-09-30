using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Twinbox.Hosting;

internal sealed partial class RetentionService(
    ITwinboxMaintenance maintenance,
    IOptions<TwinboxOptions> options,
    TimeProvider time,
    ILogger<RetentionService> logger) : BackgroundService
{
    private readonly ILogger _logger = logger;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var retention = options.Value.Retention;
        if (!retention.Enabled)
        {
            return;
        }

        using var timer = new PeriodicTimer(retention.CleanupInterval, time);
        do
        {
            try
            {
                await maintenance.RunCleanupAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                LogPurgeFailed(ex);
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Twinbox retention cleanup failed; will retry next interval.")]
    private partial void LogPurgeFailed(Exception error);
}
