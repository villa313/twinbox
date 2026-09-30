using System.Net;
using Microsoft.AspNetCore.Builder;

namespace Twinbox.Webhooks.Tests;

public sealed class ShopifyAndGitHubTests
{
    private const string Secret = "shpss_current";
    private const string OldSecret = "shpss_previous";
    private const string Body = """{"id":820982911946154508,"email":"jon@example.com"}""";

    [Fact]
    public async Task Shopify_ValidSignature_IsStoredWithTopicAndShop()
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/shopify", Body, ShopifyHeaders(Sign.Shopify(Secret, Body)));
        await host.DispatchAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var webhook = Assert.Single(host.Journal.Received);
        Assert.Equal(("shopify", "wh-1", "orders/create"), (webhook.Provider, webhook.EventId, webhook.EventType));
        Assert.Equal("shop.myshopify.com", webhook.Headers["X-Shopify-Shop-Domain"]);
        Assert.DoesNotContain("X-Shopify-Hmac-Sha256", webhook.Headers.Keys);
    }

    [Theory]
    [InlineData(OldSecret, Body, HttpStatusCode.OK)]
    [InlineData("wrong", Body, HttpStatusCode.Unauthorized)]
    [InlineData(Secret, """{"id":1}""", HttpStatusCode.Unauthorized)]
    public async Task Shopify_SignatureChecks(string signingSecret, string signedBody, HttpStatusCode expected)
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/shopify", Body, ShopifyHeaders(Sign.Shopify(signingSecret, signedBody)));

        Assert.Equal(expected, response.StatusCode);
        Assert.Equal(expected == HttpStatusCode.OK ? 1 : 0, host.Store.Snapshot().Count);
    }

    [Fact]
    public async Task Shopify_MissingWebhookId_IsABadRequest()
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/shopify", Body, ("X-Shopify-Hmac-Sha256", Sign.Shopify(Secret, Body)));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Shopify_GarbageSignature_IsRejected()
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/shopify", Body, ShopifyHeaders("not base64!"));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GitHub_ValidSignature_IsStoredWithDeliveryAndEvent()
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/github", Body, GitHubHeaders(Sign.GitHub(Secret, Body)));
        await host.DispatchAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var webhook = Assert.Single(host.Journal.Received);
        Assert.Equal(("github", "72d3162e-cc78-11e3-81ab-4c9367dc0958", "push"), (webhook.Provider, webhook.EventId, webhook.EventType));
    }

    [Theory]
    [InlineData(OldSecret, Body, HttpStatusCode.OK)]
    [InlineData("wrong", Body, HttpStatusCode.Unauthorized)]
    [InlineData(Secret, """{"id":2}""", HttpStatusCode.Unauthorized)]
    public async Task GitHub_SignatureChecks(string signingSecret, string signedBody, HttpStatusCode expected)
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/github", Body, GitHubHeaders(Sign.GitHub(signingSecret, signedBody)));

        Assert.Equal(expected, response.StatusCode);
    }

    [Theory]
    [InlineData("sha1=0123")]
    [InlineData("sha256=zz")]
    [InlineData("0123456789abcdef")]
    public async Task GitHub_MalformedSignature_IsRejected(string signature)
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/github", Body, GitHubHeaders(signature));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GitHub_TruncatedSignature_IsRejected()
    {
        await using var host = await StartAsync();
        var signature = Sign.GitHub(Secret, Body);

        var response = await host.PostAsync("/github", Body, GitHubHeaders(signature[..^2]));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    private static (string, string)[] ShopifyHeaders(string signature) =>
    [
        ("X-Shopify-Hmac-Sha256", signature),
        ("X-Shopify-Webhook-Id", "wh-1"),
        ("X-Shopify-Topic", "orders/create"),
        ("X-Shopify-Shop-Domain", "shop.myshopify.com"),
    ];

    private static (string, string)[] GitHubHeaders(string signature) =>
    [
        ("X-Hub-Signature-256", signature),
        ("X-GitHub-Delivery", "72d3162e-cc78-11e3-81ab-4c9367dc0958"),
        ("X-GitHub-Event", "push"),
    ];

    private static Task<WebhookHost> StartAsync() =>
        WebhookHost.StartAsync(app =>
        {
            app.MapWebhookInbox("/shopify", w => w.VerifyShopify(WebhookSecrets.Of(Secret, OldSecret)));
            app.MapWebhookInbox("/github", w => w.VerifyGitHub(WebhookSecrets.Of(Secret, OldSecret)));
        });
}
