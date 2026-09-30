using Azure.Messaging.ServiceBus;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus;

/// <summary>For hosts that receive Service Bus messages themselves, such as Azure Functions triggers, and settle them like the transport does.</summary>
public static class AzureServiceBusInbound
{
    /// <summary>The dead-letter reason used when a <see cref="PermanentDeliveryException"/> says a message can never be processed.</summary>
    public const string PermanentFailureReason = "PermanentDeliveryFailure";

    public static IncomingMessage ToIncomingMessage(ServiceBusReceivedMessage message, string source)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        return AzureServiceBusMapping.ToIncomingMessage(message, source);
    }
}
