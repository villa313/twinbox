using System.Collections.Concurrent;
using System.Net;

namespace Twinbox.Http.Tests;

/// <summary>Answers requests from a script instead of the network and records what was sent.</summary>
internal sealed class FakeHttpHandler : HttpMessageHandler
{
    private readonly ConcurrentQueue<Func<CancellationToken, Task<HttpResponseMessage>>> _responses = new();

    public List<RecordedRequest> Requests { get; } = [];

    /// <summary>Used once the scripted responses run out.</summary>
    public HttpStatusCode DefaultStatus { get; set; } = HttpStatusCode.OK;

    public FakeHttpHandler Respond(HttpStatusCode status, Action<HttpResponseMessage>? configure = null)
    {
        _responses.Enqueue(_ =>
        {
            var response = new HttpResponseMessage(status);
            configure?.Invoke(response);
            return Task.FromResult(response);
        });
        return this;
    }

    public FakeHttpHandler Respond(Func<CancellationToken, Task<HttpResponseMessage>> respond)
    {
        _responses.Enqueue(respond);
        return this;
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = request.Content is null ? null : await request.Content.ReadAsByteArrayAsync(cancellationToken);
        var headers = request.Headers
            .Concat(request.Content?.Headers ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>())
            .ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase);
        lock (Requests)
        {
            Requests.Add(new RecordedRequest(request.Method, request.RequestUri!, headers, body));
        }

        return _responses.TryDequeue(out var respond)
            ? await respond(cancellationToken)
            : new HttpResponseMessage(DefaultStatus);
    }
}

internal sealed record RecordedRequest(HttpMethod Method, Uri Url, IReadOnlyDictionary<string, string> Headers, byte[]? Body);
