using System.Net.Http.Headers;
using Twinbox.Transport;

namespace Twinbox.Http;

internal static class HttpRequests
{
    public static HttpRequestMessage Create(HttpEndpoint endpoint, TransportMessage message, DateTimeOffset now)
    {
        var options = endpoint.Options;
        var request = new HttpRequestMessage(options.Method, UrlTemplate.Expand(options.Url!, message));
        var carriesBody = options.Method != HttpMethod.Get && options.Method != HttpMethod.Head;
        var body = carriesBody ? message.Body : ReadOnlyMemory<byte>.Empty;
        if (carriesBody)
        {
            request.Content = CreateContent(body, options.ContentType ?? message.ContentType);
        }

        // Each header is only added when absent, so earlier ones take precedence.
        endpoint.Signer?.AddHeaders(request, message.MessageId, now, body.Span);
        if (!string.IsNullOrEmpty(options.IdempotencyKeyHeader))
        {
            AddHeader(request, options.IdempotencyKeyHeader, message.MessageId);
        }

        // The W3C trace context always goes, as HttpClient's own propagation does; other message headers only when asked.
        foreach (var name in (string[])[TransportHeaders.TraceParent, TransportHeaders.TraceState, TransportHeaders.Baggage])
        {
            if (message.Headers.TryGetValue(name, out var value))
            {
                AddHeader(request, name, value);
            }
        }

        foreach (var (name, value) in options.Headers)
        {
            AddHeader(request, name, value);
        }

        if (options.ForwardHeaders)
        {
            AddHeader(request, TransportHeaders.MessageId, message.MessageId);
            AddHeader(request, TransportHeaders.MessageName, message.MessageName);
            if (message.PartitionKey is { } partitionKey)
            {
                AddHeader(request, TransportHeaders.PartitionKey, partitionKey);
            }

            foreach (var (name, value) in message.Headers)
            {
                AddHeader(request, name, value);
            }
        }

        return request;
    }

    /// <summary>True when the header could be sent: a valid name and a value that cannot split the header block.</summary>
    public static bool IsSendable(string name, string value)
    {
        if (value.AsSpan().IndexOfAny('\r', '\n', '\0') >= 0)
        {
            return false;
        }

        using var probe = new HttpRequestMessage();
        using var content = new ByteArrayContent([]);
        return probe.Headers.TryAddWithoutValidation(name, value) || content.Headers.TryAddWithoutValidation(name, value);
    }

    private static ReadOnlyMemoryContent CreateContent(ReadOnlyMemory<byte> body, string contentType)
    {
        var content = new ReadOnlyMemoryContent(body);
        if (MediaTypeHeaderValue.TryParse(contentType, out var mediaType))
        {
            content.Headers.ContentType = mediaType;
        }
        else if (!string.IsNullOrWhiteSpace(contentType))
        {
            content.Headers.TryAddWithoutValidation("Content-Type", contentType.ReplaceLineEndings(" "));
        }

        return content;
    }

    private static void AddHeader(HttpRequestMessage request, string name, string value)
    {
        if (value.AsSpan().IndexOfAny('\r', '\n', '\0') >= 0
            || request.Headers.NonValidated.Contains(name)
            || request.Content?.Headers.NonValidated.Contains(name) == true)
        {
            return;
        }

        // A false result means a content header, or a name HTTP does not allow, which is skipped.
        if (!request.Headers.TryAddWithoutValidation(name, value))
        {
            request.Content?.Headers.TryAddWithoutValidation(name, value);
        }
    }
}
