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

    public IReadOnlyList<Route> Get(Type messageType) =>
        _routes.TryGetValue(messageType, out var routes) ? routes : [];
}
