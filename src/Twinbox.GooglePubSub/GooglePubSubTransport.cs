using Google.Cloud.PubSub.V1;
using Twinbox.Transport;

namespace Twinbox.GooglePubSub;

/// <summary>Publishes to the Pub/Sub topic named by the destination, a topic id or a full "projects/…/topics/…" name.</summary>
public sealed class GooglePubSubTransport : ITransport
{
    public const string TransportName = "googlepubsub";

    private readonly GooglePubSubClients _clients;

    internal GooglePubSubTransport(GooglePubSubClients clients)
    {
        _clients = clients;
    }

    public string Name => TransportName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        TopicName topic;
        try
        {
            topic = GooglePubSubMapping.ToTopicName(message.Destination, _clients.Options.ProjectId);
        }
        catch (ArgumentException ex)
        {
            throw new PermanentDeliveryException($"'{message.Destination}' is not a valid Pub/Sub topic: {ex.Message}", ex);
        }

        var outgoing = GooglePubSubMapping.ToPubsubMessage(message, _clients.Options.EnableMessageOrdering);
        try
        {
            await _clients.PublishAsync(topic, outgoing, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (GooglePubSubErrors.IsPermanent(ex))
        {
            throw new PermanentDeliveryException($"Pub/Sub rejected message {message.MessageId} for topic '{topic}': {ex.Message}", ex);
        }
        catch (Exception ex) when (GooglePubSubErrors.RetryAfter(ex) is { } delay)
        {
            throw new RetryAfterException($"Pub/Sub throttled message {message.MessageId} for topic '{topic}': {ex.Message}", delay);
        }
    }
}
