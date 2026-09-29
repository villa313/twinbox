using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Twinbox.Transport;

namespace Twinbox.Nats;

/// <summary>Publishes to the JetStream subject named by the destination, with the message id as Nats-Msg-Id.</summary>
public sealed class NatsTransport : ITransport
{
    public const string TransportName = "nats";

    private readonly NatsClients _clients;

    internal NatsTransport(NatsClients clients)
    {
        _clients = clients;
    }

    public string Name => TransportName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        PubAckResponse ack;
        try
        {
            ack = await _clients.PublishAsync(message.Destination, message.Body, NatsMapping.ToHeaders(message), cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (NatsErrors.IsPermanent(ex))
        {
            throw new PermanentDeliveryException($"NATS rejected message {message.MessageId} for subject '{message.Destination}': {ex.Message}", ex);
        }
        catch (Exception ex) when (NatsErrors.IsNoResponse(ex) && !cancellationToken.IsCancellationRequested)
        {
            if (!await _clients.IsUnroutableAsync(message.Destination, cancellationToken).ConfigureAwait(false))
            {
                throw;
            }

            var description = $"No JetStream stream captures subject '{message.Destination}', so message {message.MessageId} was not stored.";
            throw _clients.Options.DeadLetterUnroutable
                ? new PermanentDeliveryException(description, ex)
                : new InvalidOperationException(description, ex);
        }

        // A duplicate ack is success: the stream already holds this id, which is all a retried send needs.
        if (ack.Error is { } error)
        {
            var description = $"JetStream rejected message {message.MessageId} for subject '{message.Destination}': {NatsErrors.Describe(error)}";
            var rejection = new NatsJSApiException(error);
            throw NatsErrors.IsPermanent(error)
                ? new PermanentDeliveryException(description, rejection)
                : new InvalidOperationException(description, rejection);
        }
    }
}
