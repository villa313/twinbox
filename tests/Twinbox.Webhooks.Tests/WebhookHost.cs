using System.Collections.Concurrent;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Twinbox.InMemory;
using Twinbox.Webhooks;

namespace Twinbox.Webhooks.Tests;

/// <summary>A real ASP.NET Core pipeline on a TestServer, with the dispatcher driven by hand.</summary>
internal sealed class WebhookHost : IAsyncDisposable
{
    private WebhookHost(WebApplication app, FakeTimeProvider time)
    {
        App = app;
        Time = time;
        Client = app.GetTestClient();
    }

    public WebApplication App { get; }

    public FakeTimeProvider Time { get; }

    public HttpClient Client { get; }

    public Journal Journal => App.Services.GetRequiredService<Journal>();

    public InMemoryOutboxStore Store => App.Services.GetRequiredService<InMemoryOutboxStore>();

    public static async Task<WebhookHost> StartAsync(
        Action<IEndpointRouteBuilder> map,
        Action<TwinboxBuilder>? configure = null,
        IDictionary<string, string?>? configuration = null)
    {
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        if (configuration is not null)
        {
            builder.Configuration.AddInMemoryCollection(configuration);
        }

        builder.Services.AddSingleton<TimeProvider>(time).AddSingleton<Journal>();
        builder.Services.AddTwinbox(twinbox =>
        {
            twinbox.UseTestHarness().AddWebhooks().AddHandler<RecordingHandler, WebhookReceived>();
            configure?.Invoke(twinbox);
        });

        var app = builder.Build();
        map(app);
        await app.StartAsync();
        return new WebhookHost(app, time);
    }

    public Task<HttpResponseMessage> PostAsync(string path, string body, params (string Name, string Value)[] headers) =>
        PostAsync(path, new ByteArrayContent(Encoding.UTF8.GetBytes(body)), headers);

    public Task<HttpResponseMessage> PostAsync(string path, HttpContent content, params (string Name, string Value)[] headers)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, path) { Content = content };
        content.Headers.ContentType ??= new("application/json");
        foreach (var (name, value) in headers)
        {
            request.Headers.TryAddWithoutValidation(name, value);
        }

        return Client.SendAsync(request);
    }

    public Task<int> DispatchAsync() => App.Services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default);

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await App.StopAsync();
        await App.DisposeAsync();
    }
}

internal sealed class Journal
{
    private readonly ConcurrentQueue<WebhookReceived> _received = new();

    public IReadOnlyList<WebhookReceived> Received => [.. _received];

    public int FailuresLeft { get; set; }

    public void Record(WebhookReceived webhook) => _received.Enqueue(webhook);
}

internal sealed class RecordingHandler(Journal journal) : IHandle<WebhookReceived>
{
    public Task HandleAsync(WebhookReceived message, MessageContext context, CancellationToken cancellationToken)
    {
        if (journal.FailuresLeft > 0)
        {
            journal.FailuresLeft--;
            throw new InvalidOperationException("Handler failed on purpose.");
        }

        journal.Record(message);
        return Task.CompletedTask;
    }
}
