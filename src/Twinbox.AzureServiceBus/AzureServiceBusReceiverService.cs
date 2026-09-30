using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Twinbox.AzureServiceBus;

internal sealed partial class AzureServiceBusReceiverService(
    AzureServiceBusTransport transport,
    AzureServiceBusMessageHandler handler,
    IOptions<AzureServiceBusOptions> options,
    ILogger<AzureServiceBusReceiverService> logger) : BackgroundService
{
    private readonly ILogger _logger = logger;
    private readonly List<RunningProcessor> _processors = [];

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Waits for ExecuteAsync first, so the processor list is no longer being filled.
        try
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown timed out; still release the processors below rather than failing the host's shutdown.
        }

        foreach (var processor in _processors)
        {
            try
            {
                await processor.StopAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogStopFailed(ex, processor.EntityPath);
            }

            try
            {
                await processor.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogStopFailed(ex, processor.EntityPath);
            }
        }

        _processors.Clear();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        foreach (var listener in settings.Listeners)
        {
            var processor = listener.Sessions ? CreateSessionProcessor(listener, settings) : CreateProcessor(listener, settings);
            _processors.Add(processor);
            await processor.StartAsync(stoppingToken).ConfigureAwait(false);
            LogListening(listener.EntityPath);
        }
    }

    private RunningProcessor CreateProcessor(AzureServiceBusListener listener, AzureServiceBusOptions settings)
    {
        var processorOptions = new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = settings.MaxConcurrentCalls,
            PrefetchCount = settings.PrefetchCount,
        };
        var processor = listener.SubscriptionName is null
            ? transport.Client.CreateProcessor(listener.EntityName, processorOptions)
            : transport.Client.CreateProcessor(listener.EntityName, listener.SubscriptionName, processorOptions);

        var source = listener.EntityPath;
        processor.ProcessMessageAsync += async args =>
        {
            var settlement = await handler.HandleAsync(args.Message, source, args.CancellationToken).ConfigureAwait(false);
            await SettleAsync(
                settlement,
                () => args.CompleteMessageAsync(args.Message, CancellationToken.None),
                (reason, description) => args.DeadLetterMessageAsync(args.Message, reason, description, CancellationToken.None),
                () => args.AbandonMessageAsync(args.Message, cancellationToken: CancellationToken.None),
                args.CancellationToken).ConfigureAwait(false);
        };
        processor.ProcessErrorAsync += OnErrorAsync;
        return new RunningProcessor(source, processor.StartProcessingAsync, processor.StopProcessingAsync, processor);
    }

    private RunningProcessor CreateSessionProcessor(AzureServiceBusListener listener, AzureServiceBusOptions settings)
    {
        var processorOptions = new ServiceBusSessionProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentSessions = settings.MaxConcurrentCalls,
            MaxConcurrentCallsPerSession = 1,
            PrefetchCount = settings.PrefetchCount,
        };
        var processor = listener.SubscriptionName is null
            ? transport.Client.CreateSessionProcessor(listener.EntityName, processorOptions)
            : transport.Client.CreateSessionProcessor(listener.EntityName, listener.SubscriptionName, processorOptions);

        var source = listener.EntityPath;
        processor.ProcessMessageAsync += async args =>
        {
            var settlement = await handler.HandleAsync(args.Message, source, args.CancellationToken).ConfigureAwait(false);
            await SettleAsync(
                settlement,
                () => args.CompleteMessageAsync(args.Message, CancellationToken.None),
                (reason, description) => args.DeadLetterMessageAsync(args.Message, reason, description, CancellationToken.None),
                () => args.AbandonMessageAsync(args.Message, cancellationToken: CancellationToken.None),
                args.CancellationToken).ConfigureAwait(false);
        };
        processor.ProcessErrorAsync += OnErrorAsync;
        return new RunningProcessor(source, processor.StartProcessingAsync, processor.StopProcessingAsync, processor);
    }

    // Settlement ignores the processor's token: a message handled during shutdown should still be settled, not left locked.
    // The processor keeps renewing the lock while an abandon waits out its delay; stopping cuts the wait short.
    private static async Task SettleAsync(
        Settlement settlement,
        Func<Task> complete,
        Func<string?, string?, Task> deadLetter,
        Func<Task> abandon,
        CancellationToken stopping)
    {
        switch (settlement.Action)
        {
            case SettlementAction.Complete:
                await complete().ConfigureAwait(false);
                break;
            case SettlementAction.DeadLetter:
                await deadLetter(settlement.Reason, settlement.Description).ConfigureAwait(false);
                break;
            default:
                await WaitAsync(settlement.Delay, stopping).ConfigureAwait(false);
                await abandon().ConfigureAwait(false);
                break;
        }
    }

    private static async Task WaitAsync(TimeSpan delay, CancellationToken stopping)
    {
        if (delay <= TimeSpan.Zero)
        {
            return;
        }

        try
        {
            await Task.Delay(delay, stopping).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Stopping: hand it back now so another instance can take it.
        }
    }

    private Task OnErrorAsync(ProcessErrorEventArgs args)
    {
        LogProcessorError(args.Exception, args.EntityPath, args.ErrorSource);
        return Task.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Listening on Azure Service Bus entity {EntityPath}.")]
    private partial void LogListening(string entityPath);

    [LoggerMessage(Level = LogLevel.Error, Message = "Azure Service Bus processor for {EntityPath} reported an error during {ErrorSource}.")]
    private partial void LogProcessorError(Exception error, string entityPath, ServiceBusErrorSource errorSource);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Stopping the Azure Service Bus processor for {EntityPath} failed.")]
    private partial void LogStopFailed(Exception error, string entityPath);

    private sealed record RunningProcessor(
        string EntityPath,
        Func<CancellationToken, Task> StartAsync,
        Func<CancellationToken, Task> StopAsync,
        IAsyncDisposable Processor)
    {
        public ValueTask DisposeAsync() => Processor.DisposeAsync();
    }
}
