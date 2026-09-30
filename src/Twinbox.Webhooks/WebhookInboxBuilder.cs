using Microsoft.AspNetCore.Http;
using Twinbox.Webhooks.Verification;

namespace Twinbox.Webhooks;

/// <summary>Configures one webhook endpoint. Exactly one verifier is required: unsigned webhooks are never accepted.</summary>
public sealed class WebhookInboxBuilder
{
    private static readonly TimeSpan DefaultTolerance = TimeSpan.FromMinutes(5);

    private readonly List<string> _forwardedHeaders = [];

    internal WebhookInboxBuilder()
    {
    }

    internal IWebhookVerifier? Verifier { get; private set; }

    internal string? Provider { get; private set; }

    internal long MaxBodySize { get; private set; } = 1024 * 1024;

    internal IReadOnlyList<string> ForwardedHeaders => _forwardedHeaders;

    internal string? Store { get; private set; }

    internal Func<HttpContext, string?>? TenantResolver { get; private set; }

    /// <summary>Timestamps further than <paramref name="tolerance"/> (default 5 minutes) from now are refused as replays.</summary>
    public WebhookInboxBuilder VerifyStripe(WebhookSecrets secrets, TimeSpan? tolerance = null) =>
        Verify(new StripeVerifier(Required(secrets), Tolerance(tolerance)));

    public WebhookInboxBuilder VerifyShopify(WebhookSecrets secrets) =>
        Verify(HeaderSignatureVerifier.Shopify(Required(secrets)))
            .ForwardHeaders("X-Shopify-Topic", "X-Shopify-Shop-Domain", "X-Shopify-API-Version", "X-Shopify-Event-Id", "X-Shopify-Triggered-At");

    public WebhookInboxBuilder VerifyGitHub(WebhookSecrets secrets) =>
        Verify(HeaderSignatureVerifier.GitHub(Required(secrets)))
            .ForwardHeaders("X-GitHub-Event", "X-GitHub-Hook-ID", "X-GitHub-Hook-Installation-Target-ID", "X-GitHub-Hook-Installation-Target-Type");

    /// <summary>Standard Webhooks, also sent with <c>svix-*</c> headers. Secrets look like <c>whsec_&lt;base64&gt;</c>.</summary>
    public WebhookInboxBuilder VerifyStandardWebhooks(WebhookSecrets secrets, TimeSpan? tolerance = null) =>
        Verify(new StandardWebhooksVerifier(Required(secrets), Tolerance(tolerance)));

    public WebhookInboxBuilder VerifyHmac(WebhookSecrets secrets, Action<HmacSignatureOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new HmacSignatureOptions();
        configure(options);
        if (string.IsNullOrWhiteSpace(options.SignatureHeader))
        {
            throw new ArgumentException("VerifyHmac needs a SignatureHeader.", nameof(configure));
        }

        return Verify(new HeaderSignatureVerifier(Required(secrets), options, "hmac"))
            .ForwardHeaders(options.EventTypeHeader is { } type ? [type] : []);
    }

    public WebhookInboxBuilder Verify(IWebhookVerifier verifier)
    {
        ArgumentNullException.ThrowIfNull(verifier);
        if (Verifier is not null)
        {
            throw new InvalidOperationException("This webhook endpoint already has a verifier.");
        }

        Verifier = verifier;
        return this;
    }

    /// <summary>Names the sender: it becomes the destination (<c>webhooks/{provider}</c>) and scopes event-id deduplication.</summary>
    public WebhookInboxBuilder WithProvider(string provider)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(provider);
        Provider = provider;
        return this;
    }

    /// <summary>Copies these request headers into <see cref="WebhookReceived.Headers"/>. They aren't covered by the signature.</summary>
    public WebhookInboxBuilder ForwardHeaders(params string[] names)
    {
        ArgumentNullException.ThrowIfNull(names);
        foreach (var name in names)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            if (!_forwardedHeaders.Contains(name, StringComparer.OrdinalIgnoreCase))
            {
                _forwardedHeaders.Add(name);
            }
        }

        return this;
    }

    /// <summary>The <see cref="Storage.IOutboxStore.Name"/> to store webhooks in; required only when several stores are registered.</summary>
    public WebhookInboxBuilder WithStore(string store)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(store);
        Store = store;
        return this;
    }

    /// <summary>Required with UseTenants: picks the tenant a request belongs to, e.g. from a route value. Null or empty answers 404.</summary>
    public WebhookInboxBuilder WithTenant(Func<HttpContext, string?> resolveTenant)
    {
        ArgumentNullException.ThrowIfNull(resolveTenant);
        TenantResolver = resolveTenant;
        return this;
    }

    /// <summary>Larger bodies get 413 before they are fully read. The default is 1 MB.</summary>
    public WebhookInboxBuilder WithMaxBodySize(long bytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(bytes);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(bytes, Array.MaxLength);
        MaxBodySize = bytes;
        return this;
    }

    private static WebhookSecrets Required(WebhookSecrets secrets)
    {
        ArgumentNullException.ThrowIfNull(secrets);
        return secrets;
    }

    private static TimeSpan Tolerance(TimeSpan? tolerance)
    {
        if (tolerance is { } value)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero, nameof(tolerance));
        }

        return tolerance ?? DefaultTolerance;
    }
}
