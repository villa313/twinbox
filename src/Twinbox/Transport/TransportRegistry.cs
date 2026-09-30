namespace Twinbox.Transport;

internal sealed class TransportRegistry
{
    private readonly Dictionary<string, ITransport> _transports;
    private readonly string? _onlyTransport;

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

        // Resolved once: every send without an explicit transport asks for it.
        _onlyTransport = _transports.Count == 1 ? _transports.Keys.First() : null;
    }

    public IEnumerable<string> Names => _transports.Keys;

    public bool TryGet(string name, out ITransport transport) =>
        _transports.TryGetValue(name, out transport!);

    public string ResolveDefaultName() => _onlyTransport ?? _transports.Count switch
    {
        0 => throw new InvalidOperationException(SetupMessages.NoTransport),
        _ => throw new InvalidOperationException(
            $"Several transports are registered ({string.Join(", ", _transports.Keys)}), so routes must name one: Route<T>().To(\"destination\", transport: \"...\")."),
    };
}
