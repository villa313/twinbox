using Twinbox.Transport;

namespace Twinbox.EventHubs;

/// <summary>Sends to the event hub named by the destination, keyed by the partition key so each key stays ordered.</summary>
public sealed class EventHubsTransport : ITransport
{
    public const string TransportName = "eventhubs";

    private readonly EventHubsClients _clients;

    internal EventHubsTransport(EventHubsClients clients)
    {
        _clients = clients;
    }

    public string Name => TransportName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        try
        {
            await _clients.SendAsync(message.Destination, EventHubsMapping.ToEventData(message), message.PartitionKey, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (EventHubsErrors.IsPermanent(ex))
        {
            throw new PermanentDeliveryException(
                $"Azure Event Hubs rejected message {message.MessageId} for '{message.Destination}': {ex.Message}", ex);
        }
    }
}
