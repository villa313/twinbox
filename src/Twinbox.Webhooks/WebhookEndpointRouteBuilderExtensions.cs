using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Messaging;
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

        var endpoint = new WebhookEndpoint(
            options.Provider ?? verifier.DefaultProvider,
            verifier,
            options.ForwardedHeaders,
            options.MaxBodySize,
            // Registering the name here lets the inbound pipeline resolve it even if only a base-type handler exists.
            services.GetRequiredService<MessageTypeRegistry>().GetOrAdd(typeof(TWebhook)),
            typeof(TWebhook),
            new TWebhook());

        return endpoints
            .MapPost(pattern, (RequestDelegate)(context => ingress.ReceiveAsync(context, endpoint)))
            .WithDisplayName($"Webhook inbox {pattern}");
    }
}
