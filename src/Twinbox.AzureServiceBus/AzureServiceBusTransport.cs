using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus;

/// <summary>Sends to queues and topics named by route destinations; owns the <see cref="ServiceBusClient"/> it is given.</summary>
public sealed class AzureServiceBusTransport : ITransport, IAsyncDisposable, IDisposable
{
    public const string TransportName = "azureservicebus";

    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);
    private readonly bool _sendSessionIds;

    internal AzureServiceBusTransport(ServiceBusClient client, AzureServiceBusOptions options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        Client = client;
        _sendSessionIds = options.SendSessionIds;
    }

    public string Name => TransportName;

    internal ServiceBusClient Client { get; }

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        var sender = _senders.GetOrAdd(message.Destination, static (destination, client) => client.CreateSender(destination), Client);
        try
        {
            await sender.SendMessageAsync(AzureServiceBusMapping.ToServiceBusMessage(message, _sendSessionIds), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (AzureServiceBusMapping.IsPermanentSendFailure(ex))
        {
            throw new PermanentDeliveryException(
                $"Azure Service Bus rejected message {message.MessageId} for '{message.Destination}': {ex.Message}", ex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var sender in _senders.Values)
        {
            await sender.DisposeAsync().ConfigureAwait(false);
        }

        _senders.Clear();
        await Client.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();
}
