namespace Twinbox.Inbox;

internal sealed class HandlerRegistry(IEnumerable<HandlerDescriptor> descriptors)
{
    private readonly Dictionary<Type, HandlerDescriptor[]> _byMessageType = descriptors
        .GroupBy(d => d.MessageType)
        .ToDictionary(g => g.Key, g => g.ToArray());

    public IReadOnlyList<HandlerDescriptor> For(Type messageType) =>
        _byMessageType.TryGetValue(messageType, out var handlers) ? handlers : [];
}
