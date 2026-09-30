using Microsoft.AspNetCore.Http;

namespace Twinbox.Webhooks;

/// <summary>Checks a webhook's signature over the raw body and extracts its event id. Must compare signatures in constant time.</summary>
public interface IWebhookVerifier
{
    /// <summary>Used when the endpoint doesn't set one with <see cref="WebhookInboxBuilder.WithProvider"/>.</summary>
    string DefaultProvider { get; }

    WebhookVerification Verify(WebhookRequest request);
}

public sealed class WebhookRequest
{
    public required IHeaderDictionary Headers { get; init; }

    /// <summary>The body bytes exactly as received; verify against these, never a re-serialized form.</summary>
    public required ReadOnlyMemory<byte> Body { get; init; }

    public required DateTimeOffset ReceivedAt { get; init; }

    public required IServiceProvider Services { get; init; }
}

/// <summary>The outcome of <see cref="IWebhookVerifier.Verify"/>. Reasons are logged, never sent back to the caller.</summary>
public sealed class WebhookVerification
{
    private WebhookVerification(int statusCode, string? eventId, string? eventType, string? reason)
    {
        StatusCode = statusCode;
        EventId = eventId;
        EventType = eventType;
        Reason = reason;
    }

    public bool Succeeded => StatusCode == StatusCodes.Status200OK;

    public int StatusCode { get; }

    public string? EventId { get; }

    public string? EventType { get; }

    public string? Reason { get; }

    public static WebhookVerification Verified(string eventId, string? eventType = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(eventId);
        return new(StatusCodes.Status200OK, eventId, eventType, null);
    }

    /// <summary>A missing, malformed, stale or wrong signature; answered with 401.</summary>
    public static WebhookVerification Unauthorized(string reason) => new(StatusCodes.Status401Unauthorized, null, null, reason);

    /// <summary>Correctly signed but unusable, e.g. without an event id; answered with 400.</summary>
    public static WebhookVerification Malformed(string reason) => new(StatusCodes.Status400BadRequest, null, null, reason);
}
