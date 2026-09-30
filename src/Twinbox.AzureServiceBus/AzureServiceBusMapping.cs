using System.Globalization;
using Azure.Messaging.ServiceBus;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus;

internal static class AzureServiceBusMapping
{
    private const string FallbackContentType = "application/octet-stream";

    public static ServiceBusMessage ToServiceBusMessage(TransportMessage message, bool sendSessionIds)
    {
        ArgumentNullException.ThrowIfNull(message);
        var result = new ServiceBusMessage(message.Body)
        {
            MessageId = message.MessageId,
            Subject = message.MessageName,
            ContentType = message.ContentType,
        };

        foreach (var header in message.Headers)
        {
            result.ApplicationProperties[header.Key] = header.Value;
        }

        if (message.PartitionKey is { } partitionKey)
        {
            result.ApplicationProperties[TransportHeaders.PartitionKey] = partitionKey;
        }

        if (sendSessionIds)
        {
            // Session-enabled entities reject messages without one; the message id gives an unkeyed message a session of its own.
            result.SessionId = message.PartitionKey ?? message.MessageId;
        }

        return result;
    }

    public static IncomingMessage ToIncomingMessage(ServiceBusReceivedMessage message, string source)
    {
        ArgumentNullException.ThrowIfNull(message);
        var headers = new Dictionary<string, string>(message.ApplicationProperties.Count, StringComparer.Ordinal);
        foreach (var property in message.ApplicationProperties)
        {
            if (ToHeaderValue(property.Value) is { } value)
            {
                headers[property.Key] = value;
            }
        }

        var messageName = !string.IsNullOrEmpty(message.Subject)
            ? message.Subject
            : headers.GetValueOrDefault(TransportHeaders.MessageName, string.Empty);

        return new IncomingMessage(
            message.MessageId,
            messageName,
            source,
            message.Body.ToMemory(),
            string.IsNullOrEmpty(message.ContentType) ? FallbackContentType : message.ContentType,
            headers,
            message.DeliveryCount,
            headers.GetValueOrDefault(TransportHeaders.PartitionKey) ?? PartitionKeyFromSession(message));
    }

    /// <summary>
    /// True for failures a retry cannot fix, so the outbox dead-letters instead of backing off. Access errors stay
    /// transient: an RBAC or key fix should release the backlog rather than find it dead-lettered.
    /// </summary>
    public static bool IsPermanentSendFailure(Exception exception) =>
        exception is ServiceBusException { Reason: ServiceBusFailureReason.MessagingEntityNotFound or ServiceBusFailureReason.MessageSizeExceeded };

    // A session id equal to the message id is the fallback given to unkeyed messages, not a partition key.
    private static string? PartitionKeyFromSession(ServiceBusReceivedMessage message) =>
        string.IsNullOrEmpty(message.SessionId) || message.SessionId == message.MessageId ? null : message.SessionId;

    private static string? ToHeaderValue(object? value) => value switch
    {
        null => null,
        string text => text,
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };
}
