using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus;

internal enum SettlementAction
{
    Complete = 0,
    DeadLetter = 1,
    Abandon = 2,
}

/// <summary>How to settle a message; <see cref="Delay"/> is how long to hold it before abandoning.</summary>
internal readonly record struct Settlement(SettlementAction Action, string? Reason = null, string? Description = null, TimeSpan Delay = default)
{
    public static Settlement Complete { get; } = new(SettlementAction.Complete);

    public static Settlement Abandon { get; } = new(SettlementAction.Abandon);
}

/// <summary>Runs a received message through the inbound pipeline and decides how the broker should settle it.</summary>
internal sealed partial class AzureServiceBusMessageHandler(
    IInboundPipeline pipeline,
    IOptions<AzureServiceBusOptions> options,
    ILogger<AzureServiceBusMessageHandler> logger)
{
    public const string PermanentFailureReason = AzureServiceBusInbound.PermanentFailureReason;

    private readonly ILogger _logger = logger;

    public async Task<Settlement> HandleAsync(ServiceBusReceivedMessage message, string source, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        try
        {
            await pipeline.ProcessAsync(AzureServiceBusMapping.ToIncomingMessage(message, source), cancellationToken).ConfigureAwait(false);
            return Settlement.Complete;
        }
        catch (PermanentDeliveryException ex)
        {
            LogDeadLettering(ex, message.MessageId, source);
            InboundDiagnostics.RecordDeadLettered(AzureServiceBusTransport.TransportName, source);
            return new Settlement(SettlementAction.DeadLetter, PermanentFailureReason, ex.Message);
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
            return Settlement.Abandon;
        }
        catch (Exception ex)
        {
            var delay = RetryDelay(options.Value, message.DeliveryCount);
            LogAbandoning(ex, message.MessageId, source, message.DeliveryCount, delay);
            return Settlement.Abandon with { Delay = delay };
        }
    }

    internal static TimeSpan RetryDelay(AzureServiceBusOptions options, int attempt) =>
        RetryBackoff.Delay(options.RetryDelay, options.MaxRetryDelay, attempt);

    [LoggerMessage(Level = LogLevel.Error, Message = "Dead-lettering message {MessageId} from {Source}: it cannot be processed.")]
    private partial void LogDeadLettering(Exception error, string messageId, string source);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Processing message {MessageId} from {Source} failed on attempt {DeliveryAttempt}; abandoning it for redelivery in {Delay}.")]
    private partial void LogAbandoning(Exception error, string messageId, string source, int deliveryAttempt, TimeSpan delay);
}
