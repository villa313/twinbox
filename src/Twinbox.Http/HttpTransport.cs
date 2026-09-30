using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.Http;

/// <summary>Sends each message as one HTTP request to the endpoint named by its destination.</summary>
public sealed partial class HttpTransport : ITransport
{
    public const string TransportName = "http";

    private const string HttpClientNamePrefix = "twinbox-http:";

    private readonly IHttpClientFactory _clients;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly Dictionary<string, HttpEndpoint> _endpoints;
    private readonly string? _destinationPrefix;

    internal HttpTransport(
        IHttpClientFactory clients,
        IOptions<HttpOptions> options,
        IOptions<TwinboxOptions> twinboxOptions,
        TimeProvider time,
        ILogger<HttpTransport> logger)
    {
        _clients = clients;
        _time = time;
        _logger = logger;
        _endpoints = options.Value.Endpoints.ToDictionary(e => e.Key, e => HttpEndpoint.Create(e.Key, e.Value), StringComparer.OrdinalIgnoreCase);
        _destinationPrefix = twinboxOptions.Value.DestinationPrefix;
    }

    public string Name => TransportName;

    /// <summary>The IHttpClientFactory client used for <paramref name="endpoint"/>; configure it with AddHttpClient.</summary>
    public static string HttpClientName(string endpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return HttpClientNamePrefix + endpoint;
    }

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        var endpoint = FindEndpoint(message.Destination)
            ?? throw new PermanentDeliveryException($"No HTTP endpoint named '{message.Destination}' is configured.");

        using var request = HttpRequests.Create(endpoint, message, _time.GetUtcNow());
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (endpoint.Options.Timeout is { } limit)
        {
            timeout.CancelAfter(limit);
        }

        HttpResponseMessage response;
        try
        {
            response = await _clients.CreateClient(endpoint.ClientName)
                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            // Our timeout or HttpClient.Timeout: the request may still have landed, which the idempotency key covers.
            throw new TimeoutException($"Endpoint '{endpoint.Name}' did not answer message {message.MessageId} in time.", ex);
        }

        using (response)
        {
            await EnsureDeliveredAsync(endpoint, message, response, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task EnsureDeliveredAsync(HttpEndpoint endpoint, TransportMessage message, HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var status = (int)response.StatusCode;
        var outcome = HttpResponses.Classify(status, endpoint.Options);
        if (outcome == HttpOutcome.Delivered)
        {
            LogDelivered(message.MessageId, endpoint.Name, status);
            return;
        }

        var snippet = await HttpResponses.ReadSnippetAsync(response.Content, cancellationToken).ConfigureAwait(false);
        var description = $"Endpoint '{endpoint.Name}' answered {status} {response.ReasonPhrase} to message {message.MessageId}"
            + (snippet.Length == 0 ? "." : $": {snippet}");

        Exception error = outcome switch
        {
            HttpOutcome.Permanent => new PermanentDeliveryException(description),
            _ when HttpResponses.RetryAfter(response, _time.GetUtcNow()) is { } retryAfter => new RetryAfterException(description, retryAfter),
            _ => new HttpRequestException(description, null, response.StatusCode),
        };
        error.Data[nameof(HttpStatusCode)] = status;
        throw error;
    }

    private HttpEndpoint? FindEndpoint(string destination)
    {
        if (_endpoints.TryGetValue(destination, out var endpoint))
        {
            return endpoint;
        }

        // Routes get Twinbox:DestinationPrefix prepended, while endpoints keep their plain names.
        return !string.IsNullOrEmpty(_destinationPrefix)
            && destination.StartsWith(_destinationPrefix, StringComparison.Ordinal)
            && _endpoints.TryGetValue(destination[_destinationPrefix.Length..], out endpoint)
                ? endpoint
                : null;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Delivered message {MessageId} to HTTP endpoint {Endpoint} ({StatusCode}).")]
    private partial void LogDelivered(string messageId, string endpoint, int statusCode);
}
