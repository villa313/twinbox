namespace Twinbox.Messaging;

internal sealed record Route(string Destination, string? Transport);

internal sealed class RouteTable
{
    private readonly Dictionary<Type, List<Route>> _routes = [];

    public void Add(Type messageType, Route route)
    {
        if (!_routes.TryGetValue(messageType, out var routes))
        {
            routes = [];
            _routes[messageType] = routes;
        }

        routes.Add(route);
    }

    /// <summary>Exact routes win; otherwise the nearest base class with routes, then an implemented interface with routes.</summary>
    public IReadOnlyList<Route> Get(Type messageType)
    {
        for (var type = messageType; type is not null; type = type.BaseType)
        {
            if (_routes.TryGetValue(type, out var routes))
            {
                return routes;
            }
        }

        foreach (var (contract, routes) in _routes)
        {
            if (contract.IsInterface && contract.IsAssignableFrom(messageType))
            {
                return routes;
            }
        }

        return [];
    }
}
