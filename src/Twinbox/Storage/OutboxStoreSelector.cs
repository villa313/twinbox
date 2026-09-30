namespace Twinbox.Storage;

internal static class OutboxStoreSelector
{
    /// <summary>Picks the store a feature writes to: the only one registered, or the one named explicitly.</summary>
    public static IOutboxStore Select(IEnumerable<IOutboxStore> stores, string? name, string feature)
    {
        var all = stores.ToArray();
        var names = string.Join(", ", all.Select(s => s.Name));
        if (name is null)
        {
            return all.Length switch
            {
                1 => all[0],
                0 => throw new InvalidOperationException($"{feature} needs an outbox store, but none is registered."),
                _ => throw new InvalidOperationException(
                    $"Several outbox stores are registered ({names}); set the Store option of {feature} to the one it should write to."),
            };
        }

        var matches = all.Where(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase)).ToArray();
        return matches.Length switch
        {
            1 => matches[0],
            0 => throw new InvalidOperationException($"{feature} is set to write to the outbox store '{name}', but the registered stores are: {names}."),
            _ => throw new InvalidOperationException($"{feature} is set to write to the outbox store '{name}', but more than one store has that name."),
        };
    }
}
