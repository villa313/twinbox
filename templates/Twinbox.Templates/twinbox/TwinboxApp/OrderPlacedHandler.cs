using Twinbox;

namespace TwinboxApp;

public sealed class OrderPlacedHandler(ILogger<OrderPlacedHandler> logger) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        // Runs once per message id, even when the broker delivers it again.
        logger.LogInformation("Order {OrderId} placed for {Total}", message.OrderId, message.Total);
        return Task.CompletedTask;
    }
}
