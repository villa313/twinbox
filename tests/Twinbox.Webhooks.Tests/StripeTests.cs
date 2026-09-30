using System.Net;
using Microsoft.AspNetCore.Builder;

namespace Twinbox.Webhooks.Tests;

public sealed class StripeTests
{
    private const string Secret = "whsec_current";
    private const string OldSecret = "whsec_previous";
    private const string Body = """{"id":"evt_1","object":"event","type":"invoice.paid","data":{"object":{"id":"in_1","type":"nested"}}}""";

    [Fact]
    public async Task ValidSignature_IsStoredAndHandled()
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/stripe", Body, ("Stripe-Signature", Sign.Stripe(Secret, Now(host), Body)));
        await host.DispatchAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var webhook = Assert.Single(host.Journal.Received);
        Assert.Equal(("stripe", "evt_1", "invoice.paid", Body), (webhook.Provider, webhook.EventId, webhook.EventType, webhook.Body));
        Assert.Equal("webhooks/stripe", Assert.Single(host.Store.Snapshot()).Destination);
    }

    [Fact]
    public async Task TamperedBody_IsRejected()
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/stripe", Body.Replace("evt_1", "evt_2", StringComparison.Ordinal), ("Stripe-Signature", Sign.Stripe(Secret, Now(host), Body)));

        await AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task WrongSecret_IsRejected()
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/stripe", Body, ("Stripe-Signature", Sign.Stripe("whsec_other", Now(host), Body)));

        await AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Theory]
    [InlineData(-301)]
    [InlineData(301)]
    public async Task TimestampOutsideTolerance_IsRejectedEvenWhenCorrectlySigned(int offsetSeconds)
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/stripe", Body, ("Stripe-Signature", Sign.Stripe(Secret, Now(host) + offsetSeconds, Body)));

        await AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task TimestampWithinTolerance_IsAccepted()
    {
        await using var host = await StartAsync();

        var response = await host.PostAsync("/stripe", Body, ("Stripe-Signature", Sign.Stripe(Secret, Now(host) - 299, Body)));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task RotatedSecret_BothOldAndNewAreAccepted()
    {
        await using var host = await StartAsync();
        const string other = """{"id":"evt_2","type":"invoice.paid"}""";

        var withOld = await host.PostAsync("/stripe", Body, ("Stripe-Signature", Sign.Stripe(OldSecret, Now(host), Body)));
        var withNew = await host.PostAsync("/stripe", other, ("Stripe-Signature", Sign.Stripe(Secret, Now(host), other)));

        Assert.Equal(HttpStatusCode.OK, withOld.StatusCode);
        Assert.Equal(HttpStatusCode.OK, withNew.StatusCode);
        Assert.Equal(2, host.Store.Snapshot().Count);
    }

    [Fact]
    public async Task SeveralV1Signatures_AnyMatchingOneIsEnough()
    {
        await using var host = await StartAsync();
        var now = Now(host);
        var header = $"t={now},v1={new string('a', 64)},v1={Sign.StripeV1(Secret, now, Body)},v0=ignored";

        var response = await host.PostAsync("/stripe", Body, ("Stripe-Signature", header));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("v1=abcd")]
    [InlineData("t=notanumber,v1=abcd")]
    [InlineData("t=1767225600")]
    public async Task MissingOrMalformedHeader_IsRejected(string? header)
    {
        await using var host = await StartAsync();

        var response = header is null
            ? await host.PostAsync("/stripe", Body)
            : await host.PostAsync("/stripe", Body, ("Stripe-Signature", header));

        await AssertRejectedAsync(host, response, HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task SignedBodyWithoutAnId_IsABadRequest()
    {
        await using var host = await StartAsync();
        const string body = """{"type":"invoice.paid"}""";

        var response = await host.PostAsync("/stripe", body, ("Stripe-Signature", Sign.Stripe(Secret, Now(host), body)));

        await AssertRejectedAsync(host, response, HttpStatusCode.BadRequest);
    }

    internal static async Task AssertRejectedAsync(WebhookHost host, HttpResponseMessage response, HttpStatusCode expected)
    {
        Assert.Equal(expected, response.StatusCode);
        Assert.Empty(await response.Content.ReadAsByteArrayAsync());
        Assert.Empty(host.Store.Snapshot());
    }

    private static long Now(WebhookHost host) => host.Time.GetUtcNow().ToUnixTimeSeconds();

    private static Task<WebhookHost> StartAsync() =>
        WebhookHost.StartAsync(app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret, OldSecret))));
}
