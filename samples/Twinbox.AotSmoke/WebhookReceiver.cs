using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Twinbox.AotSmoke;

/// <summary>Stands in for the webhook's receiver, so the HTTP transport runs end to end without a network.</summary>
public sealed class WebhookReceiver(Ledger ledger) : HttpMessageHandler
{
    private static readonly byte[] Key = [.. Enumerable.Range(1, 32).Select(i => (byte)i)];

    private static readonly ConcurrentQueue<string> RequestErrors = new();

    public static string Secret { get; } = "whsec_" + Convert.ToBase64String(Key);

    public static IEnumerable<string> Errors => RequestErrors;

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = request.Content is null ? [] : await request.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        var id = Header(request, "webhook-id");
        var timestamp = Header(request, "webhook-timestamp");
        var idempotencyKey = Header(request, "Idempotency-Key");

        if (request.Method != HttpMethod.Post || request.RequestUri?.AbsolutePath != $"/hooks/{id}" || idempotencyKey != id)
        {
            RequestErrors.Enqueue($"unexpected request {request.Method} {request.RequestUri} (webhook-id {id}, Idempotency-Key {idempotencyKey})");
            return new HttpResponseMessage(HttpStatusCode.BadRequest);
        }

        if (Header(request, "webhook-signature") != Sign($"{id}.{timestamp}.", body))
        {
            RequestErrors.Enqueue($"bad webhook signature for {id}");
            return new HttpResponseMessage(HttpStatusCode.Unauthorized);
        }

        var shipped = JsonSerializer.Deserialize(body, SmokeJsonContext.Default.OrderShipped);
        if (shipped is null || shipped.TrackingNumber != $"TRACK-{shipped.OrderId}")
        {
            RequestErrors.Enqueue($"bad webhook body for {id}: {Encoding.UTF8.GetString(body)}");
            return new HttpResponseMessage(HttpStatusCode.BadRequest);
        }

        ledger.Record("shipment", shipped.OrderId, id!);
        return new HttpResponseMessage(HttpStatusCode.OK);
    }

    private static string? Header(HttpRequestMessage request, string name) =>
        request.Headers.TryGetValues(name, out var values) ? values.FirstOrDefault() : null;

    private static string Sign(string prefix, byte[] body)
    {
        using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, Key);
        hmac.AppendData(Encoding.UTF8.GetBytes(prefix));
        hmac.AppendData(body);
        return "v1," + Convert.ToBase64String(hmac.GetHashAndReset());
    }
}
