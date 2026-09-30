using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.Http.Tests;

public sealed class RegistrationTests
{
    [Fact]
    public async Task UseHttp_RegistersTheHttpTransport()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/orders")));

        var transport = Assert.Single(host.Services.GetServices<ITransport>(), t => t is HttpTransport);
        Assert.Equal("http", transport.Name);
        Assert.Equal(HttpTransport.TransportName, transport.Name);
        Assert.Same(host.Transport, transport);
    }

    [Fact]
    public async Task Configuration_DefinesEndpoints()
    {
        await using var host = HttpTestHost.Create(null, configuration: new Dictionary<string, string?>
        {
            ["Twinbox:Http:Endpoints:valuelink:Url"] = "https://api.vendor.test/{tenant}/orders",
            ["Twinbox:Http:Endpoints:valuelink:Method"] = "patch",
            ["Twinbox:Http:Endpoints:valuelink:Headers:X-Api-Key"] = "k-123",
            ["Twinbox:Http:Endpoints:valuelink:Timeout"] = "00:00:05",
            ["Twinbox:Http:Endpoints:valuelink:ContentType"] = "application/vnd.vendor+json",
            ["Twinbox:Http:Endpoints:valuelink:ForwardHeaders"] = "true",
            ["Twinbox:Http:Endpoints:valuelink:IdempotencyKeyHeader"] = "X-Request-Id",
            ["Twinbox:Http:Endpoints:valuelink:WebhookSecret"] = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
            ["Twinbox:Http:Endpoints:valuelink:TransientStatusCodes:0"] = "423",
            ["Twinbox:Http:Endpoints:valuelink:SuccessStatusCodes:0"] = "409",
        });

        var endpoint = Endpoint(host, "valuelink");

        Assert.Equal("https://api.vendor.test/{tenant}/orders", endpoint.Url!.OriginalString);
        Assert.Equal(HttpMethod.Patch, endpoint.Method);
        Assert.Equal("k-123", endpoint.Headers["x-api-key"]);
        Assert.Equal(TimeSpan.FromSeconds(5), endpoint.Timeout);
        Assert.Equal("application/vnd.vendor+json", endpoint.ContentType);
        Assert.True(endpoint.ForwardHeaders);
        Assert.Equal("X-Request-Id", endpoint.IdempotencyKeyHeader);
        Assert.NotNull(endpoint.WebhookSecret);
        Assert.Contains(423, endpoint.TransientStatusCodes);
        Assert.Contains(409, endpoint.SuccessStatusCodes);
    }

    [Fact]
    public async Task Configuration_OverridesCodeOnlyWhereSet()
    {
        await using var host = HttpTestHost.Create(
            o => o.AddEndpoint("vendor", e =>
            {
                e.Url = new Uri("https://code.test/orders");
                e.Headers["X-Api-Key"] = "from-code";
                e.Headers["X-Client"] = "twinbox";
                e.Timeout = TimeSpan.FromSeconds(3);
            }),
            configuration: new Dictionary<string, string?>
            {
                ["Twinbox:Http:Endpoints:vendor:Url"] = "https://config.test/orders",
                ["Twinbox:Http:Endpoints:vendor:Headers:X-Api-Key"] = "from-config",
            });

        var endpoint = Endpoint(host, "vendor");

        Assert.Equal(new Uri("https://config.test/orders"), endpoint.Url);
        Assert.Equal("from-config", endpoint.Headers["X-Api-Key"]);
        Assert.Equal("twinbox", endpoint.Headers["X-Client"]);
        Assert.Equal(TimeSpan.FromSeconds(3), endpoint.Timeout);
    }

    [Fact]
    public async Task ConfiguredEndpoint_Sends()
    {
        await using var host = HttpTestHost.Create(null, configuration: new Dictionary<string, string?>
        {
            ["Twinbox:Http:Endpoints:vendor:Url"] = "https://api.vendor.test/orders",
            ["Twinbox:Http:Endpoints:vendor:SuccessStatusCodes:0"] = "409",
        });
        host.Handler.Respond(HttpStatusCode.Conflict);

        await host.SendAsync(HttpTestHost.Message());

        Assert.Equal(new Uri("https://api.vendor.test/orders"), Assert.Single(host.Handler.Requests).Url);
    }

    [Theory]
    [MemberData(nameof(InvalidEndpoints))]
    public async Task InvalidEndpoint_FailsValidationNamingTheSetting(string name)
    {
        var (configure, setting) = InvalidEndpointsByName[name];
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", configure));

        var error = Assert.Throws<OptionsValidationException>(() => host.Services.GetRequiredService<IOptions<HttpOptions>>().Value);

        Assert.Contains($"Twinbox:Http:Endpoints:vendor:{setting}", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Validation_NeverEchoesHeaderValues()
    {
        var options = new HttpOptions().AddEndpoint("vendor", e =>
        {
            e.Url = new Uri("https://api.vendor.test");
            e.Headers["X-Api-Key"] = "secret\r\nvalue";
        });

        var result = new HttpOptionsValidator().Validate(null, options);

        Assert.True(result.Failed);
        Assert.DoesNotContain("secret", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void InvalidConfiguredUrl_IsReported()
    {
        var error = Assert.Throws<InvalidOperationException>(() => HttpTestHost.Create(null, configuration: new Dictionary<string, string?>
        {
            ["Twinbox:Http:Endpoints:vendor:Url"] = "http://[bad",
        }).Services.GetRequiredService<IOptions<HttpOptions>>().Value);

        Assert.Contains("Twinbox:Http:Endpoints:vendor:Url", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddEndpoint_ValidatesArguments()
    {
        var options = new HttpOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.AddEndpoint(" ", _ => { }));
        Assert.Throws<ArgumentNullException>(() => options.AddEndpoint("vendor", null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => new HttpEndpointOptions().TreatAsTransient(99));
        Assert.Throws<ArgumentOutOfRangeException>(() => new HttpEndpointOptions().TreatAsSuccess(600));
    }

    public static TheoryData<string> InvalidEndpoints() => [.. InvalidEndpointsByName.Keys];

    private static readonly Dictionary<string, (Action<HttpEndpointOptions> Configure, string Setting)> InvalidEndpointsByName = new()
    {
        ["no url"] = (_ => { }, "Url"),
        ["relative url"] = (e => e.Url = new Uri("/orders", UriKind.Relative), "Url"),
        ["ftp url"] = (e => e.Url = new Uri("ftp://files.test/in"), "Url"),
        ["zero timeout"] = (e => { e.Url = new Uri("https://a.test"); e.Timeout = TimeSpan.Zero; }, "Timeout"),
        ["bad secret"] = (e => { e.Url = new Uri("https://a.test"); e.WebhookSecret = "hunter2"; }, "WebhookSecret"),
        ["bad header name"] = (e => { e.Url = new Uri("https://a.test"); e.Headers["bad name"] = "x"; }, "Headers:bad name"),
        ["bad idempotency header"] = (e => { e.Url = new Uri("https://a.test"); e.IdempotencyKeyHeader = "bad name"; }, "IdempotencyKeyHeader"),
    };

    private static HttpEndpointOptions Endpoint(HttpTestHost host, string name) =>
        host.Services.GetRequiredService<IOptions<HttpOptions>>().Value.Endpoints[name];
}
