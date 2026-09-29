namespace Twinbox.Transport;

internal sealed class TransportRegistry
{
    private readonly Dictionary<string, ITransport> _transports;

    public TransportRegistry(IEnumerable<ITransport> transports)
    {
        _transports = new Dictionary<string, ITransport>(StringComparer.OrdinalIgnoreCase);
        foreach (var transport in transports)
        {
            if (!_transports.TryAdd(transport.Name, transport))
            {
                throw new InvalidOperationException($"More than one transport is registered with the name '{transport.Name}'.");
            }
        }
    }

    public bool TryGet(string name, out ITransport transport) =>
        _transports.TryGetValue(name, out transport!);

    public string ResolveDefaultName() => _transports.Count switch
    {
        1 => _transports.Keys.First(),
        0 => throw new InvalidOperationException("No Twinbox transport is registered. Add one, e.g. UseAzureServiceBus(...) or UseInMemory()."),
        _ => throw new InvalidOperationException(
            $"Several transports are registered ({string.Join(", ", _transports.Keys)}), so routes must name one: Route<T>().To(\"destination\", transport: \"...\")."),
    };
}
