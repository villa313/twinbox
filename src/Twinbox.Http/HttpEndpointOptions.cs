using Microsoft.Extensions.DependencyInjection;

namespace Twinbox.Http;

public sealed class HttpEndpointOptions
{
    public const string DefaultIdempotencyKeyHeader = "Idempotency-Key";

    private readonly HashSet<int> _transientStatusCodes = [];
    private readonly HashSet<int> _successStatusCodes = [];

    /// <summary>Absolute http(s) URL; its path and query may hold placeholders such as {tenant}, {partitionKey},
    /// {messageId} or any header name, filled per message. A message missing one is dead-lettered.</summary>
    public Uri? Url { get; set; }

    public HttpMethod Method { get; set; } = HttpMethod.Post;

    /// <summary>Sent on every request, e.g. an API key; they win over forwarded message headers.</summary>
    public IDictionary<string, string> Headers { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>Caps one request; keep it below Twinbox:Dispatcher:SendTimeout, which applies regardless.</summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>Overrides the message's own content type, for vendors that insist on a specific one.</summary>
    public string? ContentType { get; set; }

    /// <summary>Also sends the message's Twinbox headers (name, tenant, correlation id...); off by default so
    /// internal details stay private. traceparent is always sent.</summary>
    public bool ForwardHeaders { get; set; }

    /// <summary>Header carrying the message id, which most vendor APIs deduplicate retries on; null or empty omits it.</summary>
    public string? IdempotencyKeyHeader { get; set; } = DefaultIdempotencyKeyHeader;

    /// <summary>A "whsec_" base64 secret; when set, requests carry Standard Webhooks signature headers
    /// (webhook-id, webhook-timestamp, webhook-signature) for receivers to verify.</summary>
    public string? WebhookSecret { get; set; }

    /// <summary>Customizes the named client "twinbox-http:{name}", e.g. with an auth DelegatingHandler. Applied at
    /// registration, so it only works in code; configuration-only endpoints can use AddHttpClient directly.</summary>
    public Action<IHttpClientBuilder>? ConfigureHttpClient { get; set; }

    internal IReadOnlySet<int> TransientStatusCodes => _transientStatusCodes;

    internal IReadOnlySet<int> SuccessStatusCodes => _successStatusCodes;

    /// <summary>Retries these status codes, on top of 408, 429 and 5xx, for vendors that report busy states oddly.</summary>
    public HttpEndpointOptions TreatAsTransient(params int[] statusCodes) => AddStatusCodes(_transientStatusCodes, statusCodes);

    /// <summary>Counts these status codes as delivered, e.g. 409 from a vendor rejecting a repeat it already applied.</summary>
    public HttpEndpointOptions TreatAsSuccess(params int[] statusCodes) => AddStatusCodes(_successStatusCodes, statusCodes);

    private HttpEndpointOptions AddStatusCodes(HashSet<int> target, int[] statusCodes)
    {
        ArgumentNullException.ThrowIfNull(statusCodes);
        foreach (var code in statusCodes)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(code, 100, nameof(statusCodes));
            ArgumentOutOfRangeException.ThrowIfGreaterThan(code, 599, nameof(statusCodes));
            target.Add(code);
        }

        return this;
    }
}
