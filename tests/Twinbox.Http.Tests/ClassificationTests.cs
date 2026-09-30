using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Twinbox.Transport;

namespace Twinbox.Http.Tests;

public sealed class ClassificationTests
{
    public enum Expected
    {
        Delivered,
        Transient,
        Permanent,
    }

    [Theory]
    [InlineData(200, Expected.Delivered)]
    [InlineData(201, Expected.Delivered)]
    [InlineData(202, Expected.Delivered)]
    [InlineData(204, Expected.Delivered)]
    [InlineData(400, Expected.Permanent)]
    [InlineData(401, Expected.Transient)]
    [InlineData(403, Expected.Transient)]
    [InlineData(404, Expected.Permanent)]
    [InlineData(408, Expected.Transient)]
    [InlineData(409, Expected.Permanent)]
    [InlineData(422, Expected.Permanent)]
    [InlineData(429, Expected.Transient)]
    [InlineData(500, Expected.Transient)]
    [InlineData(502, Expected.Transient)]
    [InlineData(503, Expected.Transient)]
    [InlineData(504, Expected.Transient)]
    public async Task StatusCode_IsClassified(int status, Expected expected)
    {
        await using var host = CreateHost();
        host.Handler.Respond((HttpStatusCode)status);

        var error = await Record.ExceptionAsync(() => host.SendAsync(HttpTestHost.Message()));

        switch (expected)
        {
            case Expected.Delivered:
                Assert.Null(error);
                break;
            case Expected.Permanent:
                Assert.IsType<PermanentDeliveryException>(error);
                Assert.Equal(status, error.Data["HttpStatusCode"]);
                break;
            default:
                Assert.Equal(status, Assert.IsType<HttpRequestException>(error).StatusCode is { } code ? (int)code : 0);
                break;
        }
    }

    [Fact]
    public async Task RetryAfterSeconds_BecomesARetryAfterHint()
    {
        await using var host = CreateHost();
        host.Handler.Respond(HttpStatusCode.TooManyRequests, r => r.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(120)));

        var error = await Assert.ThrowsAsync<RetryAfterException>(() => host.SendAsync(HttpTestHost.Message()));

        Assert.Equal(TimeSpan.FromSeconds(120), error.RetryAfter);
        Assert.Contains("429", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RetryAfterDate_IsMeasuredFromNow()
    {
        await using var host = CreateHost();
        host.Handler.Respond(
            HttpStatusCode.ServiceUnavailable,
            r => r.Headers.RetryAfter = new RetryConditionHeaderValue(HttpTestHost.Start.AddMinutes(5)));

        var error = await Assert.ThrowsAsync<RetryAfterException>(() => host.SendAsync(HttpTestHost.Message()));

        Assert.Equal(TimeSpan.FromMinutes(5), error.RetryAfter);
    }

    [Fact]
    public async Task RetryAfterDateInThePast_MeansNoExtraWait()
    {
        await using var host = CreateHost();
        host.Handler.Respond(
            HttpStatusCode.ServiceUnavailable,
            r => r.Headers.RetryAfter = new RetryConditionHeaderValue(HttpTestHost.Start.AddMinutes(-5)));

        var error = await Assert.ThrowsAsync<RetryAfterException>(() => host.SendAsync(HttpTestHost.Message()));

        Assert.Equal(TimeSpan.Zero, error.RetryAfter);
    }

    [Fact]
    public async Task RetryAfterOnAPermanentStatus_IsStillPermanent()
    {
        await using var host = CreateHost();
        host.Handler.Respond(HttpStatusCode.BadRequest, r => r.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(5)));

        await Assert.ThrowsAsync<PermanentDeliveryException>(() => host.SendAsync(HttpTestHost.Message()));
    }

    [Fact]
    public async Task Overrides_WinOverTheDefaults()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e => e
            .TreatAsSuccess(409)
            .TreatAsTransient(423, 400)
            .Url = new Uri("https://api.vendor.test/orders")));
        host.Handler.Respond(HttpStatusCode.Conflict).Respond((HttpStatusCode)423).Respond(HttpStatusCode.BadRequest);

        await host.SendAsync(HttpTestHost.Message());
        await Assert.ThrowsAsync<HttpRequestException>(() => host.SendAsync(HttpTestHost.Message()));
        await Assert.ThrowsAsync<HttpRequestException>(() => host.SendAsync(HttpTestHost.Message()));
    }

    [Fact]
    public async Task ErrorMessage_CarriesASanitizedTruncatedSnippetButNotTheRequest()
    {
        await using var host = HttpTestHost.Create(o => o.AddEndpoint("vendor", e =>
        {
            e.Url = new Uri("https://api.vendor.test/orders?api_key=secret-in-url");
            e.Headers["X-Api-Key"] = "secret-in-header";
        }));
        var body = "{\"error\":\"invalid\",\r\n\t\"detail\":\"" + new string('x', 2000) + "\"}";
        host.Handler.Respond(HttpStatusCode.UnprocessableEntity, r => r.Content = new StringContent(body, Encoding.UTF8, "application/json"));

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(() =>
            host.SendAsync(HttpTestHost.Message(body: """{"card":"4111111111111111"}""")));

        Assert.Contains("422", error.Message, StringComparison.Ordinal);
        Assert.Contains("{\"error\":\"invalid\", \"detail\":\"xxx", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain('\n', error.Message);
        Assert.DoesNotContain("4111", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("secret", error.Message, StringComparison.Ordinal);
        var snippet = error.Message[(error.Message.IndexOf(": ", StringComparison.Ordinal) + 2)..];
        Assert.Equal(HttpResponses.MaxSnippetLength, snippet.Length);
        Assert.EndsWith("…", snippet, StringComparison.Ordinal);
    }

    [Fact]
    public void Sanitize_CollapsesControlCharactersAndWhitespace() =>
        Assert.Equal("a b c", HttpResponses.Sanitize("  a\r\n\tb\u0000\u001b c  "));

    private static HttpTestHost CreateHost() =>
        HttpTestHost.Create(o => o.AddEndpoint("vendor", e => e.Url = new Uri("https://api.vendor.test/orders")));
}
