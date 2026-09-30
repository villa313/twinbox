using Twinbox.Transport;

namespace Twinbox.Chaos.Tests;

/// <summary>Records every send in a shared log; <paramref name="fault"/> runs first and may throw, stall or hang.</summary>
public sealed class ChaosTransport(
    string instance,
    DeliveryLog log,
    Func<TransportMessage, CancellationToken, Task>? fault = null) : ITransport
{
    public const string TransportName = "chaos";

    public string Name => TransportName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        log.RecordCall(message);
        if (fault is not null)
        {
            await fault(message, cancellationToken);
        }

        log.RecordDelivery(message, instance);
    }
}
