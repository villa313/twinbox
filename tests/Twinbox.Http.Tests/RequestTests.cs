using System.Net;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Transport;

namespace Twinbox.Http.Tests;

public sealed class RequestTests
{
    private const string TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    [Fact]
    public async Task Send_PostsThePayloadWithItsContentTypeAndIdempotencyKey()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/orders")));
        var message = HttpTestHost.Message();

        await host.SendAsync(message);

        var request = Assert.Single(host.Handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("https://api.vendor.test/orders", request.Url.ToString());
        Assert.Equal("""{"orderId":1}""", Encoding.UTF8.GetString(request.Body!));
        Assert.Equal("application/json", request.Headers["Content-Type"]);
        Assert.Equal(message.MessageId, request.Headers["Idempotency-Key"]);
    }

    [Fact]
    public async Task Send_FillsUrlPlaceholdersFromTheMessageAndItsHeaders()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e =>
            e.Url = new Uri("https://api.vendor.test/tenants/{tenant}/orders/{partitionKey}?id={messageId}&type={messageName}&c={twinbox-correlation-id}")));
        var message = HttpTestHost.Message(
            headers: new Dictionary<string, string>
            {
                [TransportHeaders.TenantId] = "acme corp",
                [TransportHeaders.CorrelationId] = "c/1",
            },
            partitionKey: "order-42");

        await host.SendAsync(message);

        Assert.Equal(
            $"https://api.vendor.test/tenants/acme%20corp/orders/order-42?id={message.MessageId}&type=order-placed&c=c%2F1",
            Assert.Single(host.Handler.Requests).Url.AbsoluteUri);
    }

    [Fact]
    public async Task Send_MissingPlaceholderValue_IsPermanent()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/{tenant}/orders")));

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(() => host.SendAsync(HttpTestHost.Message()));

        Assert.Contains("{tenant}", error.Message, StringComparison.Ordinal);
        Assert.Empty(host.Handler.Requests);
    }

    [Fact]
    public async Task Send_AlwaysForwardsTraceParentButOtherHeadersOnlyWhenAsked()
    {
        await using var host = HttpTestHost.Create(o => o
            .AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/orders"))
            .AddEndpoint("hooks", e =>
            {
                e.Url = new Uri("https://hooks.test/in");
                e.ForwardHeaders = true;
            }));
        var headers = new Dictionary<string, string>
        {
            [TransportHeaders.TraceParent] = TraceParent,
            [TransportHeaders.TenantId] = "acme",
            ["x-bad"] = "split\r\nInjected: yes",
        };

        await host.SendAsync(HttpTestHost.Message("vendor", headers: headers, partitionKey: "p1"));
        await host.SendAsync(HttpTestHost.Message("hooks", headers: headers, partitionKey: "p1"));

        var vendor = host.Handler.Requests[0].Headers;
        Assert.Equal(TraceParent, vendor["traceparent"]);
        Assert.False(vendor.ContainsKey(TransportHeaders.TenantId));
        Assert.False(vendor.ContainsKey(TransportHeaders.MessageName));

        var hooks = host.Handler.Requests[1].Headers;
        Assert.Equal(TraceParent, hooks["traceparent"]);
        Assert.Equal("acme", hooks[TransportHeaders.TenantId]);
        Assert.Equal("order-placed", hooks[TransportHeaders.MessageName]);
        Assert.Equal("p1", hooks[TransportHeaders.PartitionKey]);
        Assert.False(hooks.ContainsKey("x-bad"));
        Assert.False(hooks.ContainsKey("Injected"));
    }

    [Fact]
    public async Task Send_AppliesStaticHeadersMethodContentTypeAndIdempotencyHeaderName()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e =>
        {
            e.Url = new Uri("https://api.vendor.test/orders");
            e.Method = HttpMethod.Put;
            e.Headers["X-Api-Key"] = "k-123";
            e.Headers["Content-Language"] = "en";
            e.ContentType = "application/vnd.vendor+json";
            e.IdempotencyKeyHeader = "X-Request-Id";
            e.ForwardHeaders = true;
        }));
        var message = HttpTestHost.Message(headers: new Dictionary<string, string> { ["X-Api-Key"] = "from-message" });

        await host.SendAsync(message);

        var request = Assert.Single(host.Handler.Requests);
        Assert.Equal(HttpMethod.Put, request.Method);
        Assert.Equal("k-123", request.Headers["X-Api-Key"]);
        Assert.Equal("en", request.Headers["Content-Language"]);
        Assert.Equal("application/vnd.vendor+json", request.Headers["Content-Type"]);
        Assert.Equal(message.MessageId, request.Headers["X-Request-Id"]);
        Assert.False(request.Headers.ContainsKey("Idempotency-Key"));
    }

    [Fact]
    public async Task Send_WithoutIdempotencyHeader_OmitsIt()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e =>
        {
            e.Url = new Uri("https://api.vendor.test/orders");
            e.IdempotencyKeyHeader = null;
        }));

        await host.SendAsync(HttpTestHost.Message());

        Assert.False(Assert.Single(host.Handler.Requests).Headers.ContainsKey("Idempotency-Key"));
    }

    [Fact]
    public async Task Get_SendsNoBody()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e =>
        {
            e.Url = new Uri("https://api.vendor.test/ping/{messageId}");
            e.Method = HttpMethod.Get;
        }));

        await host.SendAsync(HttpTestHost.Message());

        Assert.Null(Assert.Single(host.Handler.Requests).Body);
    }

    [Fact]
    public async Task UnknownEndpoint_IsPermanent()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/orders")));

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(() => host.SendAsync(HttpTestHost.Message("elsewhere")));

        Assert.Contains("elsewhere", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DestinationPrefix_IsIgnoredWhenMatchingEndpoints()
    {
        await using var host = HttpTestHost.Create(
            o => o.AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/orders")),
            b => b.Configure(o => o.DestinationPrefix = "staging-"));

        await host.SendAsync(HttpTestHost.Message("staging-vendor"));

        Assert.Single(host.Handler.Requests);
    }

    [Fact]
    public async Task EndpointTimeout_IsTransient()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e =>
        {
            e.Url = new Uri("https://api.vendor.test/orders");
            e.Timeout = TimeSpan.FromMilliseconds(50);
        }));
        host.Handler.Respond(async ct =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return new HttpResponseMessage(HttpStatusCode.OK);
        });

        var error = await Assert.ThrowsAsync<TimeoutException>(() => host.SendAsync(HttpTestHost.Message()));

        Assert.Contains("vendor", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NetworkFailure_IsTransient()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/orders")));
        host.Handler.Respond(_ => throw new HttpRequestException("Connection refused"));

        await Assert.ThrowsAsync<HttpRequestException>(() => host.SendAsync(HttpTestHost.Message()));
    }

    [Fact]
    public async Task CallerCancellation_IsNotReportedAsTimeout()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/orders")));
        host.Handler.Respond(async ct =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return new HttpResponseMessage(HttpStatusCode.OK);
        });
        using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => host.Transport.SendAsync(HttpTestHost.Message(), cancel.Token));
    }

    [Fact]
    public async Task ConfigureHttpClient_CustomizesTheEndpointsNamedClient()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e =>
        {
            e.Url = new Uri("https://api.vendor.test/orders");
            e.ConfigureHttpClient = c => c.AddHttpMessageHandler(() => new StampHandler("Bearer t0k3n"));
        }));

        await host.SendAsync(HttpTestHost.Message());

        Assert.Equal("Bearer t0k3n", Assert.Single(host.Handler.Requests).Headers["Authorization"]);
        Assert.Equal("twinbox-http:vendor", HttpTransport.HttpClientName("vendor"));
    }

    private sealed class StampHandler(string authorization) : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            request.Headers.TryAddWithoutValidation("Authorization", authorization);
            return base.SendAsync(request, cancellationToken);
        }
    }
}
