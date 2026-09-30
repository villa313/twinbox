using System.Net;
using Microsoft.AspNetCore.Builder;

namespace Twinbox.Webhooks.Tests;

public sealed class StandardWebhooksTests
{
    private const string Secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    private const string OldSecret = "whsec_c2VjcmV0LWtleS1vbGQtb2xkLW9sZA==";
    private const string Body = """{"type":"user.created","timestamp":"2026-01-01T00:00:00Z","data":{"id":"u_1"}}""";
    private const string Id = "msg_2KWPBgLlAfxdpx2AI54pPJ85f4W";

    [Theory]
    [InlineData("webhook-")]
    [InlineData("svix-")]
    public async Task ValidSignature_IsStoredWithTheSignedId(string headerPrefix)
    {
        await using var host = await StartAsync();
        var now = Now(host);

        var response = await host.PostAsync("/hooks", Body, Headers(headerPrefix, Id, now, Sign.StandardWebhooks(Secret, Id, now, Body)));
        await host.DispatchAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var webhook = Assert.Single(host.Journal.Received);
        Assert.Equal(("standard-webhooks", Id, "user.created"), (webhook.Provider, webhook.EventId, webhook.EventType));
    }

    [Fact]
    public async Task SignatureListWithUnknownVersions_MatchesTheV1Entry()
    {
        await using var host = await StartAsync();
        var now = Now(host);
        var list = $"v1a,c29tZXRoaW5n v1,{Convert.ToBase64String(new byte[32])} {Sign.StandardWebhooks(OldSecret, Id, now, Body)}";

        var response = await host.PostAsync("/hooks", Body, Headers("webhook-", Id, now, list));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ChangedId_IsRejectedBecauseTheIdIsSigned()
    {
        await using var host = await StartAsync();
        var now = Now(host);

        var response = await host.PostAsync("/hooks", Body, Headers("webhook-", "msg_other", now, Sign.StandardWebhooks(Secret, Id, now, Body)));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task TamperedBody_IsRejected()
    {
        await using var host = await StartAsync();
        var now = Now(host);

        var response = await host.PostAsync("/hooks", Body + " ", Headers("webhook-", Id, now, Sign.StandardWebhooks(Secret, Id, now, Body)));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task WrongSecret_IsRejected()
    {
        await using var host = await StartAsync();
        var now = Now(host);

        var response = await host.PostAsync("/hooks", Body, Headers("webhook-", Id, now, Sign.StandardWebhooks("whsec_d3Jvbmctc2VjcmV0", Id, now, Body)));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task StaleTimestamp_IsRejected()
    {
        await using var host = await StartAsync();
        var stale = Now(host) - 600;

        var response = await host.PostAsync("/hooks", Body, Headers("webhook-", Id, stale, Sign.StandardWebhooks(Secret, Id, stale, Body)));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task CustomTolerance_IsHonoured()
    {
        await using var host = await WebhookHost.StartAsync(app =>
            app.MapWebhookInbox("/hooks", w => w.VerifyStandardWebhooks(WebhookSecrets.Of(Secret), TimeSpan.FromMinutes(15))));
        var old = Now(host) - 600;

        var response = await host.PostAsync("/hooks", Body, Headers("webhook-", Id, old, Sign.StandardWebhooks(Secret, Id, old, Body)));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static (string, string)[] Headers(string prefix, string id, long timestamp, string signature) =>
        [(prefix + "id", id), (prefix + "timestamp", timestamp.ToString(System.Globalization.CultureInfo.InvariantCulture)), (prefix + "signature", signature)];

    private static long Now(WebhookHost host) => host.Time.GetUtcNow().ToUnixTimeSeconds();

    private static Task<WebhookHost> StartAsync() =>
        WebhookHost.StartAsync(app => app.MapWebhookInbox("/hooks", w => w.VerifyStandardWebhooks(WebhookSecrets.Of(Secret, OldSecret))));
}
