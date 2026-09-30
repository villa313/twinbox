using System.Data.Common;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.Webhooks.Tests;

public sealed class IngressTests
{
    private const string Secret = "whsec_current";
    private const string Body = """{"id":"evt_1","type":"invoice.paid"}""";

    [Fact]
    public async Task SameEventPostedTwice_IsStoredOnceAndHandledOnce()
    {
        await using var host = await StartStripeAsync();

        var first = await PostStripeAsync(host, Body);
        host.Time.Advance(TimeSpan.FromSeconds(30));
        var retry = await PostStripeAsync(host, Body);
        await host.DispatchAsync();
        await host.DispatchAsync();

        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(HttpStatusCode.OK, retry.StatusCode);
        Assert.Single(host.Store.Snapshot());
        Assert.Single(host.Journal.Received);
    }

    [Fact]
    public async Task SameEventPostedAfterDelivery_IsNotHandledAgain()
    {
        await using var host = await StartStripeAsync();

        await PostStripeAsync(host, Body);
        await host.DispatchAsync();
        var retry = await PostStripeAsync(host, Body);
        await host.DispatchAsync();

        Assert.Equal(HttpStatusCode.OK, retry.StatusCode);
        Assert.Single(host.Journal.Received);
    }

    [Fact]
    public async Task HandlerFailure_IsRetriedLater()
    {
        await using var host = await StartStripeAsync();
        host.Journal.FailuresLeft = 1;

        var response = await PostStripeAsync(host, Body);
        await host.DispatchAsync();
        Assert.Empty(host.Journal.Received);
        Assert.Equal(OutboxMessageStatus.Pending, Assert.Single(host.Store.Snapshot()).Status);

        host.Time.Advance(TimeSpan.FromMinutes(5));
        await host.DispatchAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("evt_1", Assert.Single(host.Journal.Received).EventId);
        var row = Assert.Single(host.Store.Snapshot());
        Assert.Equal((OutboxMessageStatus.Sent, 2), (row.Status, row.Attempts));
    }

    [Fact]
    public async Task TypedEndpoint_IsHandledAsItsOwnTypeAndByBaseHandlers()
    {
        await using var host = await WebhookHost.StartAsync(
            app => app.MapWebhookInbox<StripeEvent>("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret))),
            twinbox => twinbox.AddHandler<StripeEventHandler, StripeEvent>());

        await PostStripeAsync(host, Body);
        await host.DispatchAsync();

        Assert.Equal("StripeEvent", Assert.Single(host.Store.Snapshot()).MessageName);
        Assert.Equal(2, host.Journal.Received.Count);
        Assert.All(host.Journal.Received, w => Assert.IsType<StripeEvent>(w));
    }

    [Fact]
    public async Task SameEventOnTwoEndpointsOfDifferentProviders_IsStoredForEach()
    {
        await using var host = await WebhookHost.StartAsync(app =>
        {
            app.MapWebhookInbox("/a", w => w.VerifyStripe(WebhookSecrets.Of(Secret)).WithProvider("stripe-eu"));
            app.MapWebhookInbox("/b", w => w.VerifyStripe(WebhookSecrets.Of(Secret)).WithProvider("stripe-us"));
        });

        await PostStripeAsync(host, Body, "/a");
        await PostStripeAsync(host, Body, "/b");

        Assert.Equal(["webhooks/stripe-eu", "webhooks/stripe-us"], host.Store.Snapshot().Select(m => m.Destination));
    }

    [Fact]
    public async Task BodyOverTheLimit_IsRejectedWith413()
    {
        await using var host = await WebhookHost.StartAsync(app =>
            app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret)).WithMaxBodySize(64)));
        var body = $$"""{"id":"evt_1","padding":"{{new string('x', 100)}}"}""";

        var response = await PostStripeAsync(host, body);

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task BodyOverTheLimitWithoutContentLength_IsRejectedWhileReading()
    {
        await using var host = await WebhookHost.StartAsync(app =>
            app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret))));
        var content = new UnknownLengthContent(new byte[(1024 * 1024) + 1]);

        var response = await host.PostAsync("/stripe", content, ("Stripe-Signature", "t=1,v1=00"));

        await StripeTests.AssertRejectedAsync(host, response, HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task SecretsFromConfiguration_AcceptEveryEntryOfARotation()
    {
        await using var host = await WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.FromConfiguration("Stripe:WebhookSecrets"))),
            configuration: new Dictionary<string, string?>
            {
                ["Stripe:WebhookSecrets:0"] = "whsec_new",
                ["Stripe:WebhookSecrets:1"] = Secret,
            });

        var withOld = await PostStripeAsync(host, Body);
        var withNew = await PostStripeAsync(host, Body, secret: "whsec_new");
        var withOther = await PostStripeAsync(host, Body, secret: "whsec_other");

        Assert.Equal(
            [HttpStatusCode.OK, HttpStatusCode.OK, HttpStatusCode.Unauthorized],
            [withOld.StatusCode, withNew.StatusCode, withOther.StatusCode]);
    }

    [Fact]
    public async Task MissingConfiguredSecret_FailsClosed()
    {
        await using var host = await WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.FromConfiguration("Stripe:Missing"))));

        var response = await PostStripeAsync(host, Body);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Empty(host.Store.Snapshot());
    }

    [Theory]
    [InlineData(WebhookHmacAlgorithm.Sha256, SignatureEncoding.Hex)]
    [InlineData(WebhookHmacAlgorithm.Sha512, SignatureEncoding.Base64)]
    public async Task GenericHmac_VerifiesTheConfiguredScheme(WebhookHmacAlgorithm algorithm, SignatureEncoding encoding)
    {
        await using var host = await WebhookHost.StartAsync(app =>
            app.MapWebhookInbox("/acme", w => w
                .VerifyHmac(WebhookSecrets.Of("acme-secret"), o =>
                {
                    o.SignatureHeader = "X-Acme-Signature";
                    o.Algorithm = algorithm;
                    o.Encoding = encoding;
                    o.Prefix = "sig=";
                    o.EventIdHeader = "X-Acme-Id";
                    o.EventTypeHeader = "X-Acme-Event";
                })
                .WithProvider("acme")));
        var key = Encoding.UTF8.GetBytes("acme-secret");
        var bytes = Encoding.UTF8.GetBytes(Body);
        var mac = algorithm == WebhookHmacAlgorithm.Sha512
            ? System.Security.Cryptography.HMACSHA512.HashData(key, bytes)
            : System.Security.Cryptography.HMACSHA256.HashData(key, bytes);
        var signature = "sig=" + (encoding == SignatureEncoding.Hex ? Sign.Hex(mac) : Convert.ToBase64String(mac));

        var valid = await host.PostAsync("/acme", Body, ("X-Acme-Signature", signature), ("X-Acme-Id", "a-1"), ("X-Acme-Event", "thing.done"));
        var unprefixed = await host.PostAsync("/acme", Body, ("X-Acme-Signature", signature[4..]), ("X-Acme-Id", "a-2"));
        await host.DispatchAsync();

        Assert.Equal((HttpStatusCode.OK, HttpStatusCode.Unauthorized), (valid.StatusCode, unprefixed.StatusCode));
        var webhook = Assert.Single(host.Journal.Received);
        Assert.Equal(("acme", "a-1", "thing.done"), (webhook.Provider, webhook.EventId, webhook.EventType));
        Assert.Equal("webhooks/acme", Assert.Single(host.Store.Snapshot()).Destination);
    }

    [Fact]
    public async Task GenericHmacWithoutAnIdHeader_DeduplicatesIdenticalBodies()
    {
        await using var host = await WebhookHost.StartAsync(app =>
            app.MapWebhookInbox("/acme", w => w.VerifyHmac(WebhookSecrets.Of("acme-secret"), o => o.SignatureHeader = "X-Sig")));
        var signature = Sign.Hex(System.Security.Cryptography.HMACSHA256.HashData(Encoding.UTF8.GetBytes("acme-secret"), Encoding.UTF8.GetBytes(Body)));

        await host.PostAsync("/acme", Body, ("X-Sig", signature));
        var repeat = await host.PostAsync("/acme", Body, ("X-Sig", signature));

        Assert.Equal(HttpStatusCode.OK, repeat.StatusCode);
        Assert.Single(host.Store.Snapshot());
    }

    [Fact]
    public async Task ForwardedHeaders_AreCopiedAndOthersAreNot()
    {
        await using var host = await WebhookHost.StartAsync(app =>
            app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret)).ForwardHeaders("X-Request-Source")));

        await PostStripeAsync(host, Body, extra: [("X-Request-Source", "dashboard"), ("X-Other", "dropped")]);
        await host.DispatchAsync();

        var webhook = Assert.Single(host.Journal.Received);
        Assert.Equal("dashboard", Assert.Single(webhook.Headers).Value);
        Assert.Equal("application/json", webhook.ContentType);
    }

    [Fact]
    public async Task StoreFailure_Answers500SoTheProviderRetries()
    {
        await using var host = await WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret)).WithStore("Throwing")),
            twinbox => twinbox.Services.AddSingleton<IOutboxStore>(new ThrowingStore(new TimeoutException("database unavailable"))));

        var response = await PostStripeAsync(host, Body);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
    }

    [Fact]
    public async Task DuplicateKeyFromAStoreWithoutAdmin_IsTreatedAsAlreadyReceived()
    {
        var store = new ThrowingStore(new FakeDbException("23505"));
        await using var host = await WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret)).WithStore("Throwing")),
            twinbox => twinbox.Services.AddSingleton<IOutboxStore>(store));

        var response = await PostStripeAsync(host, Body);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task SeveralStoresWithoutAChoice_IsRefusedAtStartup()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret))),
            twinbox => twinbox.Services.AddSingleton<IOutboxStore>(new ThrowingStore(new TimeoutException()))));

        Assert.Contains("InMemory, Throwing", error.Message, StringComparison.Ordinal);
        Assert.Contains("WithStore", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnknownStoreName_IsRefusedAtStartup()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret)).WithStore("Orders"))));

        Assert.Contains("'Orders'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoStore_IsRefusedWithThePackagesToInstall()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddTwinbox(twinbox => twinbox.AddWebhooks());
        await using var app = builder.Build();

        var error = Assert.Throws<InvalidOperationException>(
            () => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret))));

        Assert.StartsWith("The webhook endpoint '/stripe' needs an outbox store, but none is registered.", error.Message, StringComparison.Ordinal);
        Assert.Contains("Twinbox.EntityFrameworkCore (UseEntityFrameworkCore<TContext>())", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TenancyWithoutATenantResolver_IsRefusedAtStartup()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret))),
            UseTenants));

        Assert.Contains("WithTenant", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TenantResolver_StoresTheWebhookForThatTenant()
    {
        await using var host = await WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/{tenant}/stripe", w => w
                .VerifyStripe(WebhookSecrets.Of(Secret))
                .WithTenant(context => context.Request.RouteValues["tenant"] as string)),
            UseTenants);

        var response = await PostStripeAsync(host, Body, path: "/acme/stripe");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("acme", Assert.Single(host.Store.Snapshot()).TenantId);
    }

    [Fact]
    public async Task RequestWithoutATenant_IsNotFound()
    {
        await using var host = await WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret)).WithTenant(_ => null)),
            UseTenants);

        var response = await PostStripeAsync(host, Body);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Empty(host.Store.Snapshot());
    }

    [Fact]
    public async Task DestinationPrefix_AppliesToWebhookDestinations()
    {
        await using var host = await WebhookHost.StartAsync(
            app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret))),
            twinbox => twinbox.Configure(o => o.DestinationPrefix = "staging-"));

        await PostStripeAsync(host, Body);
        await host.DispatchAsync();

        Assert.Equal("staging-webhooks/stripe", Assert.Single(host.Store.Snapshot()).Destination);
        Assert.Single(host.Journal.Received);
    }

    [Fact]
    public async Task EndpointWithoutAVerifier_IsRefusedAtStartup()
    {
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            WebhookHost.StartAsync(app => app.MapWebhookInbox("/open", w => w.WithProvider("open"))));

        Assert.Contains("no verifier", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnlyPostIsMapped()
    {
        await using var host = await StartStripeAsync();

        var response = await host.Client.GetAsync(new Uri("/stripe", UriKind.Relative));

        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
    }

    private static Task<HttpResponseMessage> PostStripeAsync(
        WebhookHost host,
        string body,
        string path = "/stripe",
        string secret = Secret,
        (string, string)[]? extra = null)
    {
        var signature = Sign.Stripe(secret, host.Time.GetUtcNow().ToUnixTimeSeconds(), body);
        return host.PostAsync(path, body, [("Stripe-Signature", signature), .. extra ?? []]);
    }

    private static Task<WebhookHost> StartStripeAsync() =>
        WebhookHost.StartAsync(app => app.MapWebhookInbox("/stripe", w => w.VerifyStripe(WebhookSecrets.Of(Secret))));

    private static void UseTenants(TwinboxBuilder twinbox) => twinbox.UseTenants(o =>
    {
        o.ListTenants = (_, _) => Task.FromResult<IReadOnlyCollection<string>>(["acme"]);
        o.EnterTenant = (_, _) => { };
        o.CurrentTenant = _ => null;
    });

    public sealed record StripeEvent : WebhookReceived;

    private sealed class StripeEventHandler(Journal journal) : IHandle<StripeEvent>
    {
        public Task HandleAsync(StripeEvent message, MessageContext context, CancellationToken cancellationToken)
        {
            journal.Record(message);
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingStore(Exception error) : IOutboxStore
    {
        public string Name => "Throwing";

        public Task AppendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken) => Task.FromException(error);

        public Task<IReadOnlyList<OutboxMessage>> ClaimAsync(OutboxClaim claim, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<OutboxMessage>>([]);

        public Task CompleteAsync(string owner, IReadOnlyList<DispatchOutcome> outcomes, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<int> PurgeAsync(OutboxPurge purge, CancellationToken cancellationToken) => Task.FromResult(0);

        public Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken) =>
            Task.FromResult(new OutboxStatistics(0, null, 0));
    }

    private sealed class FakeDbException(string sqlState) : DbException("duplicate key value violates unique constraint")
    {
        public override string SqlState => sqlState;
    }

    private sealed class UnknownLengthContent(byte[] bytes) : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => stream.WriteAsync(bytes).AsTask();

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }
}
