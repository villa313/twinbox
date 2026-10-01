using Twinbox.Inbox;
using Twinbox.Messaging;

namespace Twinbox;

internal sealed class TwinboxTopology(RouteTable routes, HandlerRegistry handlers, MessageTypeRegistry messageTypes) : ITwinboxTopology
{
    public IReadOnlyList<string> DestinationsOf(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return [.. routes.Get(messageType).Select(route => route.Destination)];
    }

    public IReadOnlyList<string> ConsumersOf(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return [.. handlers.For(messageType).Select(handler => handler.ConsumerName)];
    }

    public bool IsKnown(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return messageTypes.IsRegistered(messageType);
    }
}
