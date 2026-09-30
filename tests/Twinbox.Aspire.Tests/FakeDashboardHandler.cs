using System.Net;
using System.Text;

namespace Twinbox.Aspire.Tests;

internal sealed record RecordedRequest(HttpMethod Method, Uri Uri, string? Csrf, string? ContentType, string? Body);

/// <summary>Answers requests from a per-path table and records what was sent.</summary>
internal sealed class FakeDashboardHandler : HttpMessageHandler
{
    public const string Token = "token-123";

    public const string DefaultConfig = """{"csrfToken":"token-123","readOnly":false,"showPayloads":false,"stores":[{"id":0,"name":"EntityFrameworkCore","browsable":true}],"tenants":[]}""";

    private readonly Dictionary<string, Func<RecordedRequest, HttpResponseMessage>> _routes = new(StringComparer.Ordinal);

    public List<RecordedRequest> Requests { get; } = [];

    public FakeDashboardHandler On(string path, Func<RecordedRequest, HttpResponseMessage> respond)
    {
        _routes[path] = respond;
        return this;
    }

    public FakeDashboardHandler On(string path, HttpStatusCode status, string body, string mediaType = "application/json") =>
        On(path, _ => Response(status, body, mediaType));

    public static FakeDashboardHandler Healthy(string config = DefaultConfig, int changed = 2) => new FakeDashboardHandler()
        .On("/twinbox/api/config", HttpStatusCode.OK, config)
        .On("/twinbox/api/dead/replay-all", HttpStatusCode.OK, $$"""{"changed":{{changed}}}""");

    public static HttpResponseMessage Response(HttpStatusCode status, string body, string mediaType = "application/json") =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, mediaType) };

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var recorded = new RecordedRequest(
            request.Method,
            request.RequestUri!,
            request.Headers.TryGetValues("X-Twinbox-Csrf", out var values) ? string.Join(",", values) : null,
            request.Content?.Headers.ContentType?.MediaType,
            request.Content is null ? null : await request.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
        Requests.Add(recorded);
        return _routes.TryGetValue(request.RequestUri!.AbsolutePath, out var respond)
            ? respond(recorded)
            : Response(HttpStatusCode.NotFound, string.Empty, "text/plain");
    }
}
