using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Storage;

namespace Twinbox.Hosting;

internal sealed partial class RetentionService(
    IEnumerable<IOutboxStore> outboxes,
    IOptions<TwinboxOptions> options,
    TimeProvider time,
    ILogger<RetentionService> logger,
    IInboxStore? inbox = null) : BackgroundService
{
    private readonly ILogger _logger = logger;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var retention = options.Value.Retention;
        using var timer = new PeriodicTimer(retention.CleanupInterval, time);
        do
        {
            try
            {
                await PurgeAsync(retention, stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                LogPurgeFailed(ex);
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }

    internal async Task PurgeAsync(RetentionOptions retention, CancellationToken cancellationToken)
    {
        var now = time.GetUtcNow();
        var purge = new OutboxPurge(now - retention.SentMessages, now - retention.DeadMessages, retention.BatchSize);
        foreach (var outbox in outboxes)
        {
            while (await outbox.PurgeAsync(purge, cancellationToken).ConfigureAwait(false) >= retention.BatchSize)
            {
            }
        }

        if (inbox is null)
        {
            return;
        }

        var processedBefore = now - retention.InboxEntries;
        while (await inbox.PurgeAsync(processedBefore, retention.BatchSize, cancellationToken).ConfigureAwait(false) >= retention.BatchSize)
        {
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Twinbox retention cleanup failed; will retry next interval.")]
    private partial void LogPurgeFailed(Exception error);
}
