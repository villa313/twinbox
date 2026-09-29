namespace Twinbox.Transport;

public interface ITransport
{
    /// <summary>Name routes refer to, e.g. "azureservicebus".</summary>
    string Name { get; }

    /// <summary>Throw <see cref="PermanentDeliveryException"/> for failures that retrying cannot fix.</summary>
    Task SendAsync(TransportMessage message, CancellationToken cancellationToken);
}
