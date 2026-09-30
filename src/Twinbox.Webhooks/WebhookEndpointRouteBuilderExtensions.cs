using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Twinbox;
using Twinbox.Storage;
using Twinbox.Webhooks;

namespace Microsoft.AspNetCore.Builder;

public static class WebhookEndpointRouteBuilderExtensions
{
    /// <summary>Accepts signed webhooks as <see cref="WebhookReceived"/>; handle them with <c>IHandle&lt;WebhookReceived&gt;</c>.</summary>
    public static IEndpointConventionBuilder MapWebhookInbox(
        this IEndpointRouteBuilder endpoints,
        [StringSyntax("Route")] string pattern,
        Action<WebhookInboxBuilder> configure) =>
        endpoints.MapWebhookInbox<WebhookReceived>(pattern, configure);

    /// <summary>Verifies, stores and answers 200; your <c>IHandle&lt;TWebhook&gt;</c> runs later with retries, once per provider event id.</summary>
    public static IEndpointConventionBuilder MapWebhookInbox<TWebhook>(
        this IEndpointRouteBuilder endpoints,
        [StringSyntax("Route")] string pattern,
        Action<WebhookInboxBuilder> configure)
        where TWebhook : WebhookReceived, new()
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new WebhookInboxBuilder();
        configure(options);
        var verifier = options.Verifier
            ?? throw new InvalidOperationException($"The webhook endpoint '{pattern}' has no verifier. Call VerifyStripe, VerifyGitHub, VerifyHmac or another Verify method.");

        var services = endpoints.ServiceProvider;
        var ingress = services.GetService<WebhookIngress>()
            ?? throw new InvalidOperationException("Webhooks aren't enabled. Call AddTwinbox(twinbox => twinbox.AddWebhooks()).");

        if (options.TenantResolver is null && services.GetService<TenancyOptions>() is not null)
        {
            throw new InvalidOperationException(
                $"The webhook endpoint '{pattern}' needs a tenant because UseTenants is configured. Call WithTenant(context => ...) to pick it from the request.");
        }

        var endpoint = new WebhookEndpoint(
            options.Provider ?? verifier.DefaultProvider,
            verifier,
            options.ForwardedHeaders,
            options.MaxBodySize,
            // Registering the name here lets the inbound pipeline resolve it even if only a base-type handler exists.
            services.GetRequiredService<IMessageNames>().GetName(typeof(TWebhook)),
            typeof(TWebhook),
            new TWebhook(),
            SelectStore(services.GetServices<IOutboxStore>(), options.Store, pattern),
            options.TenantResolver);

        return endpoints
            .MapPost(pattern, (RequestDelegate)(context => ingress.ReceiveAsync(context, endpoint)))
            .WithDisplayName($"Webhook inbox {pattern}");
    }

    private static IOutboxStore SelectStore(IEnumerable<IOutboxStore> stores, string? name, string pattern)
    {
        var all = stores.ToArray();
        var matches = name is null ? all : [.. all.Where(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase))];
        if (matches.Length == 1)
        {
            return matches[0];
        }

        var registered = all.Length == 0 ? "none" : string.Join(", ", all.Select(s => s.Name));
        throw new InvalidOperationException(name is null
            ? $"The webhook endpoint '{pattern}' needs exactly one outbox store to write to (registered: {registered}). Call WithStore(name) to pick one."
            : $"The webhook endpoint '{pattern}' is set to write to the outbox store '{name}', which doesn't match exactly one registered store (registered: {registered}).");
    }
}
