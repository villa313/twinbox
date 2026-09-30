using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Twinbox.AzureServiceBus;
using Twinbox.Transport;

namespace Twinbox.AzureFunctions;

/// <summary>Feeds a Service Bus trigger's message through the inbox and settles it; the trigger needs <c>AutoCompleteMessages = false</c>.</summary>
public sealed partial class TwinboxServiceBusTrigger(IInboundPipeline pipeline, ILogger<TwinboxServiceBusTrigger> logger)
{
    /// <summary>Reported to handlers as the message source when the trigger doesn't name its entity.</summary>
    public const string DefaultSource = AzureServiceBusTransport.TransportName;

    private readonly ILogger _logger = logger;

    public Task ProcessServiceBusMessageAsync(
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions,
        CancellationToken cancellationToken) =>
        ProcessServiceBusMessageAsync(message, messageActions, DefaultSource, cancellationToken);

    /// <summary>Settles the message rather than throwing, so the invocation succeeds unless settling itself fails.</summary>
    public async Task ProcessServiceBusMessageAsync(
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions messageActions,
        string source,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(messageActions);
        ArgumentException.ThrowIfNullOrWhiteSpace(source);

        // Settlement ignores the invocation's token: a message handled during shutdown should still be settled, not left locked.
        try
        {
            await pipeline.ProcessAsync(AzureServiceBusInbound.ToIncomingMessage(message, source), cancellationToken).ConfigureAwait(false);
        }
        catch (PermanentDeliveryException ex)
        {
            LogDeadLettering(ex, message.MessageId, source);
            await messageActions.DeadLetterMessageAsync(
                message,
                deadLetterReason: AzureServiceBusInbound.PermanentFailureReason,
                deadLetterErrorDescription: ex.Message,
                cancellationToken: CancellationToken.None).ConfigureAwait(false);
            return;
        }
        catch (Exception ex)
        {
            // Abandon rather than rethrow: Service Bus triggers have no host retry policy, and abandoning redelivers now
            // instead of after the lock expires, leaving poison messages to the entity's MaxDeliveryCount.
            LogAbandoning(ex, message.MessageId, source, message.DeliveryCount);
            await messageActions.AbandonMessageAsync(message, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            return;
        }

        await messageActions.CompleteMessageAsync(message, CancellationToken.None).ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Dead-lettering message {MessageId} from {Source}: it cannot be processed.")]
    private partial void LogDeadLettering(Exception error, string messageId, string source);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Processing message {MessageId} from {Source} failed on attempt {DeliveryAttempt}; abandoning for redelivery.")]
    private partial void LogAbandoning(Exception error, string messageId, string source, int deliveryAttempt);
}
