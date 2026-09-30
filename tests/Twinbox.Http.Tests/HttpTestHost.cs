using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Time.Testing;
using Twinbox.Testing;
using Twinbox.Transport;

namespace Twinbox.Http.Tests;

internal sealed class HttpTestHost : IAsyncDisposable
{
    public static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private HttpTestHost(ServiceProvider services, FakeTimeProvider time, FakeHttpHandler handler)
    {
        Services = services;
        Time = time;
        Handler = handler;
    }

    public ServiceProvider Services { get; }

    public FakeTimeProvider Time { get; }

    public FakeHttpHandler Handler { get; }

    public HttpTransport Transport => Services.GetRequiredService<HttpTransport>();

    public TwinboxTestHarness Harness => Services.GetTwinboxHarness();

    /// <summary>Every HttpClient the transport creates answers from <see cref="Handler"/>, so nothing reaches the network.</summary>
    public static HttpTestHost Create(
        Action<HttpOptions>? configure,
        Action<TwinboxBuilder>? twinbox = null,
        IDictionary<string, string?>? configuration = null)
    {
        var time = new FakeTimeProvider(Start);
        var handler = new FakeHttpHandler();
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<TimeProvider>(time);
        if (configuration is not null)
        {
            services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection(configuration).Build());
        }

        services.AddTwinbox(b =>
        {
            b.UseTestHarness().UseHttp(configure);
            twinbox?.Invoke(b);
        });
        services.ConfigureAll<HttpClientFactoryOptions>(o => o.HttpMessageHandlerBuilderActions.Add(b => b.PrimaryHandler = handler));
        return new HttpTestHost(services.BuildServiceProvider(validateScopes: true), time, handler);
    }

    public static TransportMessage Message(
        string destination = "vendor",
        string body = """{"orderId":1}""",
        IReadOnlyDictionary<string, string>? headers = null,
        string? partitionKey = null) =>
        new(
            "0190a8f2-7c3e-7a01-9f00-000000000001",
            "order-placed",
            destination,
            Encoding.UTF8.GetBytes(body),
            "application/json",
            headers ?? new Dictionary<string, string>(),
            partitionKey);

    public Task SendAsync(TransportMessage message) => Transport.SendAsync(message, TestContext.Current.CancellationToken);

    public ValueTask DisposeAsync() => Services.DisposeAsync();
}
