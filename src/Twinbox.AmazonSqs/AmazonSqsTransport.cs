using Twinbox.Transport;

namespace Twinbox.AmazonSqs;

/// <summary>Sends to the SQS queue named by the destination, or publishes to SNS for destinations starting with <see cref="TopicPrefix"/>.</summary>
public sealed class AmazonSqsTransport : ITransport
{
    public const string TransportName = "amazonsqs";

    /// <summary>Marks a destination as an SNS topic name or ARN, e.g. "sns:orders".</summary>
    public const string TopicPrefix = "sns:";

    private readonly AmazonSqsClients _clients;

    internal AmazonSqsTransport(AmazonSqsClients clients)
    {
        _clients = clients;
    }

    public string Name => TransportName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        var topic = message.Destination.StartsWith(TopicPrefix, StringComparison.Ordinal) ? StripTopicPrefix(message.Destination) : null;
        try
        {
            if (topic is not null)
            {
                await _clients.PublishAsync(message, topic, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _clients.SendToQueueAsync(message, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (AmazonSqsErrors.IsPermanent(ex))
        {
            if (topic is null && AmazonSqsErrors.IsMissingQueue(ex))
            {
                _clients.ForgetQueue(message.Destination);
            }

            var target = topic is null ? $"SQS queue '{message.Destination}'" : $"SNS topic '{topic}'";
            throw new PermanentDeliveryException($"Amazon rejected message {message.MessageId} for {target}: {ex.Message}", ex);
        }
    }

    internal static string StripTopicPrefix(string topic) =>
        topic.StartsWith(TopicPrefix, StringComparison.Ordinal) ? topic[TopicPrefix.Length..] : topic;
}
