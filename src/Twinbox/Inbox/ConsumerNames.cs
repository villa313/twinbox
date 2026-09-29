namespace Twinbox.Inbox;

internal static class ConsumerNames
{
    /// <summary>
    /// Namespace-qualified name with readable generic arguments. Type.FullName would embed assembly versions for
    /// generic handlers, silently changing the inbox key on every upgrade.
    /// </summary>
    public static string For(Type handlerType)
    {
        if (!handlerType.IsGenericType)
        {
            return handlerType.FullName ?? handlerType.Name;
        }

        var definition = handlerType.GetGenericTypeDefinition().FullName ?? handlerType.Name;
        var arity = definition.IndexOf('`', StringComparison.Ordinal);
        var name = arity < 0 ? definition : definition[..arity];
        return $"{name}<{string.Join(",", handlerType.GetGenericArguments().Select(For))}>";
    }
}
