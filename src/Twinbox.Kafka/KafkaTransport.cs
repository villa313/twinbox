using Twinbox.Transport;

namespace Twinbox.Kafka;

/// <summary>Produces to the topic named by the destination, keyed by the partition key so each key stays ordered.</summary>
public sealed class KafkaTransport : ITransport
{
    public const string TransportName = "kafka";

    private readonly KafkaClients _clients;

    internal KafkaTransport(KafkaClients clients)
    {
        _clients = clients;
    }

    public string Name => TransportName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        try
        {
            await _clients.EnsureTopicAsync(message.Destination, cancellationToken).ConfigureAwait(false);
            await _clients.ProduceAsync(message.Destination, KafkaMapping.ToKafkaMessage(message), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (KafkaErrors.IsPermanent(ex))
        {
            throw new PermanentDeliveryException($"Kafka rejected message {message.MessageId} for topic '{message.Destination}': {ex.Message}", ex);
        }
    }
}
