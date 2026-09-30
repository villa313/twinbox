using System.Net;
using Microsoft.AspNetCore.Builder;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Twinbox.Storage;
using static Twinbox.OutboxMessageStatus;

namespace Twinbox.Dashboard.Tests;

public sealed partial class DashboardTests
{
    [Theory]
    [InlineData("/twinbox/")]
    [InlineData("/twinbox/api/stats")]
    [InlineData("/twinbox/api/messages")]
    public async Task Request_WithoutAuthorizationPolicy_IsRefused(string path)
    {
        await using var host = await DashboardHost.StartAsync();

        using var response = await host.Client.GetAsync(path);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Contains("RequireAuthorization", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Map_WithoutStore_NamesThePackagesToInstall()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Services.AddTwinbox(twinbox => twinbox.UseLocalDelivery());
        await using var app = builder.Build();

        var error = Assert.Throws<InvalidOperationException>(() => app.MapTwinboxDashboard());

        Assert.StartsWith("MapTwinboxDashboard needs an outbox store, but none is registered.", error.Message, StringComparison.Ordinal);
        Assert.Contains("Twinbox.PostgreSql (UsePostgreSql(...))", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Request_WithExplicitAllowAnonymous_IsRefusedWithoutTheOption()
    {
        await using var host = await DashboardHost.StartAsync(e => e.AllowAnonymous());

        using var response = await host.Client.GetAsync("/twinbox/api/stats");

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task AllowAnonymousOption_ServesWithoutPolicy_AndLogsAWarning()
    {
        await using var host = await DashboardHost.StartAsync(configure: o => o.AllowAnonymous = true);
        host.Client.DefaultRequestHeaders.Remove(TestAuthentication.UserHeader);

        using var response = await host.Client.GetAsync("/twinbox/api/stats");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains(host.Logs.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("anonymous", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(null, null, HttpStatusCode.Unauthorized)]
    [InlineData("bob", "sales", HttpStatusCode.Forbidden)]
    [InlineData("alice", "ops", HttpStatusCode.OK)]
    public async Task Policy_IsEnforced(string? user, string? roles, HttpStatusCode expected)
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/twinbox/api/stats");
        host.Client.DefaultRequestHeaders.Clear();
        if (user is not null)
        {
            request.Headers.Add(TestAuthentication.UserHeader, user);
            request.Headers.Add(TestAuthentication.RolesHeader, roles);
        }

        using var response = await host.Client.SendAsync(request);

        Assert.Equal(expected, response.StatusCode);
    }

    [Fact]
    public async Task Page_IsHtmlWithStrictContentSecurityPolicy()
    {
        await using var host = await DashboardHost.StartSecuredAsync();

        using var response = await host.Client.GetAsync("/twinbox");
        var html = await response.Content.ReadAsStringAsync();

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("text/html", response.Content.Headers.ContentType?.MediaType);
        var csp = Assert.Single(response.Headers.GetValues("Content-Security-Policy"));
        Assert.Contains("default-src 'none'", csp, StringComparison.Ordinal);
        Assert.Contains("frame-ancestors 'none'", csp, StringComparison.Ordinal);
        var nonce = NonceSource().Match(csp).Groups[1].Value;
        Assert.NotEmpty(nonce);
        Assert.Contains($"<script nonce=\"{nonce}\">", html, StringComparison.Ordinal);
        Assert.Contains("content=\"/twinbox\"", html, StringComparison.Ordinal);
        Assert.DoesNotContain("{{", html, StringComparison.Ordinal);
        Assert.DoesNotContain("http://", html, StringComparison.Ordinal);
        Assert.DoesNotContain("https://", html, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Stats_ReportPendingDeadAndOldestAge()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        await host.SeedAsync((Dead, "orders"), (Pending, "orders"), (Sent, "orders"));

        // The seeded pending row is rescheduled into the future, so only this one, due 9 minutes ago, has an age.
        await host.Store.AppendAsync([DashboardHost.NewMessage("orders", DashboardHost.Now.AddMinutes(-9))], default);

        var stats = await host.GetJsonAsync("/twinbox/api/stats");

        var row = Assert.Single(stats["stores"]!.AsArray())!;
        Assert.Equal("InMemory", row["storeName"]!.GetValue<string>());
        Assert.Equal(2, row["pending"]!.GetValue<long>());
        Assert.Equal(1, row["dead"]!.GetValue<long>());
        Assert.Equal(9 * 60, row["oldestPendingAgeSeconds"]!.GetValue<double>());
    }

    [Fact]
    public async Task Messages_FilterByStatusDestinationAndSearch()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "orders"), (Dead, "payments"), (Sent, "orders"));

        var dead = await host.GetJsonAsync("/twinbox/api/messages?status=dead");
        var deadOrders = await host.GetJsonAsync("/twinbox/api/messages?status=Dead&destination=orders");
        var byId = await host.GetJsonAsync($"/twinbox/api/messages?search={seeded[2].Id}");

        Assert.Equal([seeded[1].Id.ToString(), seeded[0].Id.ToString()], dead.Ids());
        Assert.Equal([seeded[0].Id.ToString()], deadOrders.Ids());
        Assert.Equal([seeded[2].Id.ToString()], byId.Ids());
        Assert.Equal("broker unreachable", dead["messages"]![0]!["lastError"]!.GetValue<string>());
    }

    [Fact]
    public async Task Messages_PageWithCursor()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "a"), (Dead, "b"), (Dead, "c"));

        var first = await host.GetJsonAsync("/twinbox/api/messages?take=2");
        var cursor = first["nextCursor"]!.GetValue<string>();
        var second = await host.GetJsonAsync($"/twinbox/api/messages?take=2&cursor={Uri.EscapeDataString(cursor)}");

        Assert.Equal([seeded[2].Id.ToString(), seeded[1].Id.ToString()], first.Ids());
        Assert.Equal([seeded[0].Id.ToString()], second.Ids());
        Assert.Null(second["nextCursor"]);
    }

    [Theory]
    [InlineData("status=Lost")]
    [InlineData("status=3")]
    [InlineData("take=0")]
    [InlineData("take=500")]
    [InlineData("store=7")]
    [InlineData("tenant=acme")]
    public async Task Messages_RejectInvalidQueries(string query)
    {
        await using var host = await DashboardHost.StartSecuredAsync();

        using var response = await host.Client.GetAsync($"/twinbox/api/messages?{query}");

        Assert.True(response.StatusCode is HttpStatusCode.BadRequest or HttpStatusCode.NotFound, $"got {response.StatusCode}");
    }

    [Fact]
    public async Task Replay_MovesDeadMessagesBackToPending()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "orders"), (Pending, "orders"));

        using var response = await host.PostAsync("/twinbox/api/messages/replay", new { ids = seeded.Select(m => m.Id) });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("\"changed\":1", await response.Content.ReadAsStringAsync(), StringComparison.Ordinal);
        var replayed = await host.Store.GetAsync(seeded[0].Id, default);
        Assert.Equal(Pending, replayed!.Status);
        Assert.Equal(0, replayed.Attempts);
        Assert.Equal(DashboardHost.Now, replayed.AvailableAt);
        Assert.Null(replayed.LastError);
        Assert.Contains(host.Logs.Entries, e => e.Message.Contains("alice replayed 1", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ReplayAllDead_ReplaysEveryDeadMessageMatchingTheFilters()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "orders"), (Dead, "orders"), (Dead, "payments"), (Sent, "orders"));

        using var response = await host.PostAsync("/twinbox/api/dead/replay-all", new { destination = "orders" });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var statuses = host.Store.Snapshot().ToDictionary(m => m.Id, m => m.Status);
        Assert.Equal([Pending, Pending, Dead, Sent], seeded.Select(m => statuses[m.Id]));
    }

    [Fact]
    public async Task Delete_RemovesMessages()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "orders"), (Dead, "orders"));

        using var response = await host.PostAsync("/twinbox/api/messages/delete", new { ids = new[] { seeded[0].Id } });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal([seeded[1].Id], host.Store.Snapshot().Select(m => m.Id));
    }

    [Theory]
    [InlineData("/twinbox/api/messages/replay")]
    [InlineData("/twinbox/api/messages/delete")]
    [InlineData("/twinbox/api/dead/replay-all")]
    public async Task ReadOnlyMode_RefusesMutations(string path)
    {
        await using var host = await DashboardHost.StartSecuredAsync(o => o.ReadOnly = true);
        var seeded = await host.SeedAsync((Dead, "orders"));

        using var response = await host.PostAsync(path, new { ids = new[] { seeded[0].Id } });

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal(Dead, Assert.Single(host.Store.Snapshot()).Status);
        Assert.True((await host.GetJsonAsync("/twinbox/api/config"))["readOnly"]!.GetValue<bool>());
    }

    [Fact]
    public async Task Post_WithoutCsrfHeader_IsRefused()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "orders"));

        using var response = await host.Client.PostAsync(
            "/twinbox/api/messages/delete",
            new StringContent($$"""{"ids":["{{seeded[0].Id}}"]}""", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Single(host.Store.Snapshot());
    }

    [Fact]
    public async Task Post_WithAnotherUsersToken_IsRefused()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "orders"));
        var aliceToken = await host.CsrfTokenAsync();

        using var response = await host.PostAsync(
            "/twinbox/api/messages/delete",
            new { ids = new[] { seeded[0].Id } },
            aliceToken,
            r => r.Headers.Add(TestAuthentication.UserHeader, "mallory"));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Single(host.Store.Snapshot());
    }

    [Fact]
    public async Task Post_FromAnotherSite_IsRefusedEvenWithAToken()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "orders"));

        using var response = await host.PostAsync(
            "/twinbox/api/messages/delete",
            new { ids = new[] { seeded[0].Id } },
            tweak: r => r.Headers.Add("Sec-Fetch-Site", "cross-site"));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Single(host.Store.Snapshot());
    }

    [Fact]
    public async Task Post_WithoutJsonBody_IsRefused()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        using var request = new HttpRequestMessage(HttpMethod.Post, "/twinbox/api/messages/delete")
        {
            Content = new StringContent("ids=1", Encoding.UTF8, "application/x-www-form-urlencoded"),
        };
        request.Headers.Add("X-Twinbox-Csrf", await host.CsrfTokenAsync());

        using var response = await host.Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.UnsupportedMediaType, response.StatusCode);
    }

    [Fact]
    public async Task MessageDetail_HidesPayloadByDefault()
    {
        await using var host = await DashboardHost.StartSecuredAsync();
        var seeded = await host.SeedAsync((Dead, "orders"));

        var detail = await host.GetJsonAsync($"/twinbox/api/messages/{seeded[0].Id}");

        Assert.False(detail["payloadShown"]!.GetValue<bool>());
        Assert.Null(detail["payload"]);
        Assert.Equal("abc", detail["headers"]!["x-correlation-id"]!.GetValue<string>());
        Assert.Equal(seeded[0].Id.ToString(), detail["message"]!["id"]!.GetValue<string>());
    }

    [Fact]
    public async Task MessageDetail_ShowsPayloadCappedAt64KiB_WhenEnabled()
    {
        await using var host = await DashboardHost.StartSecuredAsync(o => o.ShowPayloads = true);
        var large = DashboardHost.NewMessage("orders", DashboardHost.Now, Encoding.UTF8.GetBytes(new string('x', 100_000)));
        await host.Store.AppendAsync([large], default);

        var detail = await host.GetJsonAsync($"/twinbox/api/messages/{large.Id}");

        Assert.True(detail["payloadShown"]!.GetValue<bool>());
        Assert.True(detail["payloadTruncated"]!.GetValue<bool>());
        Assert.Equal(64 * 1024, detail["payload"]!.GetValue<string>().Length);
    }

    [Fact]
    public async Task MessageDetail_ForUnknownId_IsNotFound()
    {
        await using var host = await DashboardHost.StartSecuredAsync();

        using var response = await host.Client.GetAsync($"/twinbox/api/messages/{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Tenants_AreListedAndBrowsedSeparately()
    {
        await using var host = await DashboardHost.StartSecuredAsync(twinbox: b =>
        {
            b.Services.AddScoped<TenantContext>();
            b.Services.AddSingleton<IOutboxStore, TenantStore>();
            b.UseTenants(o =>
            {
                o.ListTenants = (_, _) => Task.FromResult<IReadOnlyCollection<string>>(["acme", "globex"]);
                o.EnterTenant = (sp, tenant) => sp.GetRequiredService<TenantContext>().Id = tenant;
                o.CurrentTenant = sp => sp.GetRequiredService<TenantContext>().Id;
            });
        });
        var tenants = host.Services.GetServices<IOutboxStore>().OfType<TenantStore>().Single();
        await tenants.For("globex").AppendAsync([DashboardHost.NewMessage("orders", DashboardHost.Now)], default);

        var config = await host.GetJsonAsync("/twinbox/api/config");
        var stats = await host.GetJsonAsync("/twinbox/api/stats");
        var globex = await host.GetJsonAsync("/twinbox/api/messages?store=1&tenant=globex&status=");
        var acme = await host.GetJsonAsync("/twinbox/api/messages?store=1&tenant=acme");
        using var unknown = await host.Client.GetAsync("/twinbox/api/messages?store=1&tenant=initech");

        Assert.Equal(["acme", "globex"], config["tenants"]!.AsArray().Select(t => t!.GetValue<string>()));
        Assert.Equal(4, stats["stores"]!.AsArray().Count);
        Assert.Contains(stats["stores"]!.AsArray(), r => r!["tenant"]!.GetValue<string>() == "globex" && r["pending"]!.GetValue<long>() == 1);
        Assert.Single(globex.Ids());
        Assert.Empty(acme.Ids());
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);
    }

    [GeneratedRegex("script-src 'nonce-([^']+)'")]
    private static partial Regex NonceSource();

    public sealed class TenantContext
    {
        public string? Id { get; set; }
    }

    /// <summary>One in-memory store per tenant, picked from the tenant entered on the scope.</summary>
    private sealed class TenantStore(Tenancy.TwinboxScopeFactory scopes) : IOutboxStore, IOutboxAdmin
    {
        public string Name => "Tenant";

        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, InMemory.InMemoryOutboxStore> _stores = new(StringComparer.Ordinal);

        public InMemory.InMemoryOutboxStore For(string tenant) => _stores.GetOrAdd(tenant, _ => new());

        public Task AppendAsync(IReadOnlyList<OutboxMessage> messages, CancellationToken cancellationToken) => Current().AppendAsync(messages, cancellationToken);

        public Task<IReadOnlyList<OutboxMessage>> ClaimAsync(OutboxClaim claim, CancellationToken cancellationToken) => Current().ClaimAsync(claim, cancellationToken);

        public Task CompleteAsync(string owner, IReadOnlyList<DispatchOutcome> outcomes, CancellationToken cancellationToken) =>
            Current().CompleteAsync(owner, outcomes, cancellationToken);

        public Task<int> PurgeAsync(OutboxPurge purge, CancellationToken cancellationToken) => Current().PurgeAsync(purge, cancellationToken);

        public Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken) => Current().GetStatisticsAsync(cancellationToken);

        public Task<OutboxPage> QueryAsync(OutboxQuery query, CancellationToken cancellationToken) => Current().QueryAsync(query, cancellationToken);

        public Task<OutboxMessage?> GetAsync(Guid id, CancellationToken cancellationToken) => Current().GetAsync(id, cancellationToken);

        public Task<int> ReplayAsync(IReadOnlyCollection<Guid> ids, DateTimeOffset now, CancellationToken cancellationToken) =>
            Current().ReplayAsync(ids, now, cancellationToken);

        public Task<int> DeleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken) => Current().DeleteAsync(ids, cancellationToken);

        private InMemory.InMemoryOutboxStore Current()
        {
            using var scope = scopes.CreateAsyncScope();
            var tenant = scope.ServiceProvider.GetRequiredService<TenantContext>().Id
                ?? throw new InvalidOperationException("No tenant was entered.");
            return For(tenant);
        }
    }
}
