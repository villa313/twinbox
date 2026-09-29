using System.Buffers;
using Twinbox.Transport;

namespace Twinbox.Pulsar;

/// <summary>Produces to the topic named by the destination, keyed by the partition key so KeyShared subscriptions keep each key ordered.</summary>
public sealed class PulsarTransport : ITransport
{
    public const string TransportName = "pulsar";

    private readonly PulsarClients _clients;

    internal PulsarTransport(PulsarClients clients)
    {
        _clients = clients;
    }

    public string Name => TransportName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        try
        {
            await _clients.SendAsync(
                message.Destination,
                PulsarMapping.ToMetadata(message),
                new ReadOnlySequence<byte>(message.Body),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (PulsarErrors.IsPermanent(ex))
        {
            throw new PermanentDeliveryException(
                $"Pulsar rejected message {message.MessageId} for topic '{message.Destination}': {PulsarErrors.Cause(ex).Message}", ex);
        }
    }
}
