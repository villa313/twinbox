using System.Net;
using System.Text.Json.Nodes;
using Twinbox.Aspire.Hosting;

namespace Twinbox.Aspire.Tests;

public sealed class DashboardClientTests
{
    private static readonly Uri Dashboard = new("https://localhost:7001/twinbox/");

    [Fact]
    public async Task ReplayDeadLetters_FetchesTokenThenPostsWithIt()
    {
        var handler = FakeDashboardHandler.Healthy();

        var result = await ReplayAsync(handler);

        Assert.True(result.Success, result.Error);
        Assert.Equal(2, result.Replayed);
        Assert.Collection(
            handler.Requests,
            config =>
            {
                Assert.Equal(HttpMethod.Get, config.Method);
                Assert.Equal("https://localhost:7001/twinbox/api/config", config.Uri.ToString());
            },
            replay =>
            {
                Assert.Equal(HttpMethod.Post, replay.Method);
                Assert.Equal("https://localhost:7001/twinbox/api/dead/replay-all", replay.Uri.ToString());
                Assert.Equal(FakeDashboardHandler.Token, replay.Csrf);
                Assert.Equal("application/json", replay.ContentType);
                var body = JsonNode.Parse(replay.Body!)!;
                Assert.Equal(0, body["store"]!.GetValue<int>());
                Assert.Null(body["tenant"]);
            });
    }

    [Fact]
    public async Task ReplayDeadLetters_CoversEveryBrowsableStoreAndTenant()
    {
        var handler = FakeDashboardHandler.Healthy(
            """{"csrfToken":"t","readOnly":false,"stores":[{"id":0,"name":"A","browsable":true},{"id":1,"name":"B","browsable":false},{"id":2,"name":"C","browsable":true}],"tenants":["acme","globex"]}""",
            changed: 3);

        var result = await ReplayAsync(handler);

        Assert.True(result.Success, result.Error);
        Assert.Equal(12, result.Replayed);
        var targets = handler.Requests
            .Where(r => r.Method == HttpMethod.Post)
            .Select(r => JsonNode.Parse(r.Body!)!)
            .Select(b => $"{b["store"]}/{b["tenant"]}")
            .ToList();
        Assert.Equal(["0/acme", "0/globex", "2/acme", "2/globex"], targets);
    }

    [Fact]
    public async Task ReplayDeadLetters_ReadOnlyDashboard_FailsWithoutPosting()
    {
        var handler = FakeDashboardHandler.Healthy("""{"csrfToken":"t","readOnly":true,"stores":[{"id":0,"browsable":true}]}""");

        var result = await ReplayAsync(handler);

        Assert.False(result.Success);
        Assert.Contains("read-only", result.Error, StringComparison.Ordinal);
        Assert.DoesNotContain(handler.Requests, r => r.Method == HttpMethod.Post);
    }

    [Fact]
    public async Task ReplayDeadLetters_NoBrowsableStore_Fails()
    {
        var handler = FakeDashboardHandler.Healthy("""{"csrfToken":"t","readOnly":false,"stores":[{"id":0,"browsable":false}]}""");

        var result = await ReplayAsync(handler);

        Assert.False(result.Success);
        Assert.Contains("browsing", result.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplayDeadLetters_SignInPageInsteadOfConfig_ExplainsAnonymousAccess()
    {
        var handler = new FakeDashboardHandler()
            .On("/twinbox/api/config", HttpStatusCode.OK, "<html>Sign in</html>", "text/html");

        var result = await ReplayAsync(handler);

        Assert.False(result.Success);
        Assert.Contains("AllowAnonymous", result.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplayDeadLetters_Unauthorized_ExplainsAnonymousAccess()
    {
        var handler = new FakeDashboardHandler().On("/twinbox/api/config", HttpStatusCode.Unauthorized, string.Empty, "text/plain");

        var result = await ReplayAsync(handler);

        Assert.False(result.Success);
        Assert.Contains("401", result.Error, StringComparison.Ordinal);
        Assert.Contains("AllowAnonymous", result.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplayDeadLetters_NoDashboardAtPath_PointsAtThePath()
    {
        var result = await ReplayAsync(new FakeDashboardHandler());

        Assert.False(result.Success);
        Assert.Contains("404", result.Error, StringComparison.Ordinal);
        Assert.Contains("WithTwinboxDashboard", result.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplayDeadLetters_RefusedPost_ReportsDashboardError()
    {
        var handler = new FakeDashboardHandler()
            .On("/twinbox/api/config", HttpStatusCode.OK, FakeDashboardHandler.DefaultConfig)
            .On("/twinbox/api/dead/replay-all", HttpStatusCode.Forbidden, """{"error":"Missing or invalid X-Twinbox-Csrf header."}""");

        var result = await ReplayAsync(handler);

        Assert.False(result.Success);
        Assert.Equal("The Twinbox dashboard refused the request (403): Missing or invalid X-Twinbox-Csrf header.", result.Error);
    }

    [Fact]
    public async Task ReplayDeadLetters_PostAnsweredWithHtml_Fails()
    {
        var handler = new FakeDashboardHandler()
            .On("/twinbox/api/config", HttpStatusCode.OK, FakeDashboardHandler.DefaultConfig)
            .On("/twinbox/api/dead/replay-all", HttpStatusCode.OK, "<html></html>", "text/html");

        var result = await ReplayAsync(handler);

        Assert.False(result.Success);
        Assert.Contains("JSON", result.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplayDeadLetters_ServerError_ReportsStatus()
    {
        var handler = new FakeDashboardHandler().On("/twinbox/api/config", HttpStatusCode.BadGateway, string.Empty, "text/plain");

        var result = await ReplayAsync(handler);

        Assert.False(result.Success);
        Assert.Contains("502", result.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplayDeadLetters_Unreachable_ReportsConnectionFailure()
    {
        var handler = new FakeDashboardHandler().On("/twinbox/api/config", _ => throw new HttpRequestException("Connection refused"));

        var result = await ReplayAsync(handler);

        Assert.False(result.Success);
        Assert.Contains("Couldn't reach", result.Error, StringComparison.Ordinal);
        Assert.Contains("Connection refused", result.Error, StringComparison.Ordinal);
    }

    private static async Task<ReplayResult> ReplayAsync(FakeDashboardHandler handler)
    {
        using var http = new HttpClient(handler);
        return await new TwinboxDashboardClient(http).ReplayDeadLettersAsync(Dashboard, TestContext.Current.CancellationToken);
    }
}
