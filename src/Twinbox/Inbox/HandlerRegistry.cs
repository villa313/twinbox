using System.Collections.Concurrent;

namespace Twinbox.Inbox;

internal sealed class HandlerRegistry(IEnumerable<HandlerDescriptor> descriptors)
{
    private readonly HandlerDescriptor[] _descriptors = [.. descriptors];
    private readonly ConcurrentDictionary<Type, HandlerDescriptor[]> _byMessageType = new();

    /// <summary>Handlers for the type itself plus any registered for its base types or interfaces.</summary>
    public IReadOnlyList<HandlerDescriptor> For(Type messageType) =>
        _byMessageType.GetOrAdd(messageType, type => [.. _descriptors.Where(d => d.MessageType.IsAssignableFrom(type))]);
}
