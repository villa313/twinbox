using System.Net.Http.Json;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Twinbox.Dashboard;
using Twinbox.InMemory;
using Twinbox.Storage;

namespace Twinbox.Dashboard.Tests;

/// <summary>An app with the dashboard at /twinbox, a header-driven test login, and an "ops" role policy.</summary>
internal sealed class DashboardHost : IAsyncDisposable
{
    public static readonly DateTimeOffset Now = new(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);

    private readonly WebApplication _app;

    private DashboardHost(WebApplication app, HttpClient client, CapturedLogs logs)
    {
        _app = app;
        Client = client;
        Logs = logs;
    }

    public HttpClient Client { get; }

    public CapturedLogs Logs { get; }

    public IServiceProvider Services => _app.Services;

    public InMemoryOutboxStore Store => _app.Services.GetRequiredService<InMemoryOutboxStore>();

    public static async Task<DashboardHost> StartAsync(
        Action<IEndpointConventionBuilder>? secure = null,
        Action<TwinboxDashboardOptions>? configure = null,
        Action<TwinboxBuilder>? twinbox = null)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseTestServer();
        var logs = new CapturedLogs();
        builder.Logging.ClearProviders().AddProvider(logs);
        builder.Services.AddSingleton<TimeProvider>(new FakeTimeProvider(Now));
        builder.Services.AddTwinbox(b =>
        {
            b.UseInMemoryStore();
            twinbox?.Invoke(b);
        });

        // The dispatcher would deliver the seeded messages; these tests only look at them.
        builder.Services.RemoveAll<IHostedService>();
        builder.Services.AddAuthentication(TestAuthentication.Name)
            .AddScheme<AuthenticationSchemeOptions, TestAuthentication>(TestAuthentication.Name, null);
        builder.Services.AddAuthorizationBuilder().AddPolicy("ops", p => p.RequireRole("ops"));

        var app = builder.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        var endpoints = app.MapTwinboxDashboard("/twinbox", configure);
        secure?.Invoke(endpoints);
        await app.StartAsync();

        var client = app.GetTestServer().CreateClient();
        client.DefaultRequestHeaders.Add(TestAuthentication.UserHeader, "alice");
        client.DefaultRequestHeaders.Add(TestAuthentication.RolesHeader, "ops");
        return new DashboardHost(app, client, logs);
    }

    public static Task<DashboardHost> StartSecuredAsync(Action<TwinboxDashboardOptions>? configure = null, Action<TwinboxBuilder>? twinbox = null) =>
        StartAsync(e => e.RequireAuthorization("ops"), configure, twinbox);

    public async Task<OutboxMessage[]> SeedAsync(params (OutboxMessageStatus Status, string Destination)[] rows)
    {
        var messages = rows.Select((r, i) => NewMessage(r.Destination, Now.AddMinutes(-10 + i))).ToArray();
        await Store.AppendAsync(messages, default);
        await Store.ClaimAsync(new OutboxClaim("seed", messages.Length, Now, TimeSpan.FromMinutes(1)), default);
        await Store.CompleteAsync(
            "seed",
            [.. messages.Zip(rows, (m, r) => r.Status switch
            {
                OutboxMessageStatus.Dead => new DispatchOutcome(m.Id, OutboxMessageStatus.Dead, 5, Error: "broker unreachable"),
                OutboxMessageStatus.Sent => new DispatchOutcome(m.Id, OutboxMessageStatus.Sent, 1, SentAt: Now),
                _ => new DispatchOutcome(m.Id, OutboxMessageStatus.Pending, 0, AvailableAt: Now.AddHours(1)),
            })],
            default);
        return messages;
    }

    public async Task<string> CsrfTokenAsync() => (await GetJsonAsync("/twinbox/api/config"))["csrfToken"]!.GetValue<string>();

    public async Task<JsonNode> GetJsonAsync(string path)
    {
        using var response = await Client.GetAsync(path);
        response.EnsureSuccessStatusCode();
        return JsonNode.Parse(await response.Content.ReadAsStringAsync())!;
    }

    public async Task<HttpResponseMessage> PostAsync(string path, object body, string? token = null, Action<HttpRequestMessage>? tweak = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, path) { Content = JsonContent.Create(body) };
        request.Headers.Add("X-Twinbox-Csrf", token ?? await CsrfTokenAsync());
        tweak?.Invoke(request);
        return await Client.SendAsync(request);
    }

    public static OutboxMessage NewMessage(string destination, DateTimeOffset createdAt, byte[]? payload = null) => new()
    {
        Id = Guid.NewGuid(),
        MessageName = "order-placed",
        Transport = "test",
        Destination = destination,
        Payload = payload ?? Encoding.UTF8.GetBytes("""{"orderId":42}"""),
        ContentType = "application/json",
        Headers = new Dictionary<string, string> { ["x-correlation-id"] = "abc" },
        CreatedAt = createdAt,
        AvailableAt = createdAt,
    };

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _app.DisposeAsync();
    }
}

internal sealed class TestAuthentication(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string Name = "Test";
    public const string UserHeader = "X-Test-User";
    public const string RolesHeader = "X-Test-Roles";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.TryGetValue(UserHeader, out var user) || string.IsNullOrEmpty(user))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var roles = Request.Headers[RolesHeader].ToString().Split(',', StringSplitOptions.RemoveEmptyEntries);
        var identity = new ClaimsIdentity(
            [new Claim(ClaimTypes.Name, user.ToString()), .. roles.Select(r => new Claim(ClaimTypes.Role, r))],
            Name);
        return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), Name)));
    }
}

internal sealed class CapturedLogs : ILoggerProvider
{
    private readonly List<(LogLevel Level, string Message)> _entries = [];

    public IReadOnlyList<(LogLevel Level, string Message)> Entries
    {
        get
        {
            lock (_entries)
            {
                return [.. _entries];
            }
        }
    }

    public ILogger CreateLogger(string categoryName) => new Logger(this);

    public void Dispose()
    {
    }

    private sealed class Logger(CapturedLogs owner) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (owner._entries)
            {
                owner._entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }
}

internal static class JsonNodeExtensions
{
    public static string[] Ids(this JsonNode page) =>
        [.. page["messages"]!.AsArray().Select(m => m!["id"]!.GetValue<string>())];
}
