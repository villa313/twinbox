namespace Twinbox.Http;

/// <summary>Endpoints keyed by the destination name routes use; the "Twinbox:Http:Endpoints:{name}" section
/// is bound too, and its values override code.</summary>
public sealed class HttpOptions
{
    public const string SectionName = "Twinbox:Http";

    private readonly Dictionary<string, HttpEndpointOptions> _endpoints = new(StringComparer.OrdinalIgnoreCase);

    internal IReadOnlyDictionary<string, HttpEndpointOptions> Endpoints => _endpoints;

    /// <summary>Adds the endpoint named <paramref name="name"/>, or configures it further if it already exists.</summary>
    public HttpOptions AddEndpoint(string name, Action<HttpEndpointOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);
        configure(GetOrAddEndpoint(name));
        return this;
    }

    internal HttpEndpointOptions GetOrAddEndpoint(string name)
    {
        if (!_endpoints.TryGetValue(name, out var endpoint))
        {
            endpoint = new HttpEndpointOptions();
            _endpoints.Add(name, endpoint);
        }

        return endpoint;
    }
}
