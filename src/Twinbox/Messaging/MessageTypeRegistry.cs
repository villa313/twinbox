using System.Collections.Concurrent;
using System.Reflection;

namespace Twinbox.Messaging;

internal sealed class MessageTypeRegistry
{
    private readonly ConcurrentDictionary<Type, string> _names = new();
    private readonly ConcurrentDictionary<string, Type> _types = new(StringComparer.Ordinal);
    private readonly object _gate = new();

    public string GetOrAdd(Type messageType)
    {
        if (_names.TryGetValue(messageType, out var existing))
        {
            return existing;
        }

        var name = messageType.GetCustomAttribute<MessageNameAttribute>()?.Name ?? messageType.Name;
        lock (_gate)
        {
            if (_types.TryGetValue(name, out var claimedBy) && claimedBy != messageType)
            {
                throw new InvalidOperationException(
                    $"Message name '{name}' is used by both {claimedBy} and {messageType}. Give one of them a distinct [MessageName].");
            }

            _types[name] = messageType;
            _names[messageType] = name;
            return name;
        }
    }

    public bool TryResolve(string name, out Type messageType) =>
        _types.TryGetValue(name, out messageType!);
}
