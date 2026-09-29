using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus;

internal enum SettlementAction
{
    Complete = 0,
    DeadLetter = 1,
    Abandon = 2,
}

internal readonly record struct Settlement(SettlementAction Action, string? Reason = null, string? Description = null)
{
    public static Settlement Complete { get; } = new(SettlementAction.Complete);

    public static Settlement Abandon { get; } = new(SettlementAction.Abandon);
}

/// <summary>Runs a received message through the inbound pipeline and decides how the broker should settle it.</summary>
internal sealed partial class AzureServiceBusMessageHandler(IInboundPipeline pipeline, ILogger<AzureServiceBusMessageHandler> logger)
{
    public const string PermanentFailureReason = "PermanentDeliveryFailure";

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
            return new Settlement(SettlementAction.DeadLetter, PermanentFailureReason, ex.Message);
        }
        catch (Exception ex)
        {
            LogAbandoning(ex, message.MessageId, source, message.DeliveryCount);
            return Settlement.Abandon;
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Dead-lettering message {MessageId} from {Source}: it cannot be processed.")]
    private partial void LogDeadLettering(Exception error, string messageId, string source);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Processing message {MessageId} from {Source} failed on attempt {DeliveryAttempt}; abandoning for redelivery.")]
    private partial void LogAbandoning(Exception error, string messageId, string source, int deliveryAttempt);
}
