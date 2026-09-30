using Microsoft.Extensions.Configuration;

namespace Twinbox.Http;

/// <summary>The configuration shape of <see cref="HttpOptions"/>; nullable so only keys that are present override code.</summary>
internal sealed class HttpSettings
{
    public Dictionary<string, HttpEndpointSettings> Endpoints { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public static void Apply(IConfiguration configuration, HttpOptions options)
    {
        var settings = new HttpSettings();
        configuration.GetSection(HttpOptions.SectionName).Bind(settings);
        foreach (var (name, endpoint) in settings.Endpoints)
        {
            endpoint.ApplyTo(name, options.GetOrAddEndpoint(name));
        }
    }
}

internal sealed class HttpEndpointSettings
{
    public string? Url { get; set; }

    public string? Method { get; set; }

    public Dictionary<string, string> Headers { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public TimeSpan? Timeout { get; set; }

    public string? ContentType { get; set; }

    public bool? ForwardHeaders { get; set; }

    public string? IdempotencyKeyHeader { get; set; }

    public string? WebhookSecret { get; set; }

    public int[] TransientStatusCodes { get; set; } = [];

    public int[] SuccessStatusCodes { get; set; } = [];

    public void ApplyTo(string name, HttpEndpointOptions endpoint)
    {
        if (Url is not null)
        {
            endpoint.Url = Uri.TryCreate(Url, UriKind.Absolute, out var url)
                ? url
                : throw new InvalidOperationException($"{HttpOptions.SectionName}:Endpoints:{name}:Url is not an absolute URL.");
        }

        if (!string.IsNullOrWhiteSpace(Method))
        {
            endpoint.Method = new HttpMethod(Method.Trim().ToUpperInvariant());
        }

        foreach (var (header, value) in Headers)
        {
            endpoint.Headers[header] = value;
        }

        endpoint.Timeout = Timeout ?? endpoint.Timeout;
        endpoint.ContentType = ContentType ?? endpoint.ContentType;
        endpoint.ForwardHeaders = ForwardHeaders ?? endpoint.ForwardHeaders;
        endpoint.IdempotencyKeyHeader = IdempotencyKeyHeader ?? endpoint.IdempotencyKeyHeader;
        endpoint.WebhookSecret = WebhookSecret ?? endpoint.WebhookSecret;
        endpoint.TreatAsTransient(TransientStatusCodes);
        endpoint.TreatAsSuccess(SuccessStatusCodes);
    }
}
