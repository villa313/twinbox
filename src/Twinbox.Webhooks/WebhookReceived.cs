using System.Collections.ObjectModel;

namespace Twinbox.Webhooks;

/// <summary>A verified webhook. Derive a record from it (<c>record StripeEvent : WebhookReceived;</c>) to give an endpoint its own type.</summary>
[MessageName("twinbox.webhook")]
public record WebhookReceived
{
    private static readonly IReadOnlyDictionary<string, string> NoHeaders =
        new ReadOnlyDictionary<string, string>(new Dictionary<string, string>());

    public string Provider { get; init; } = string.Empty;

    /// <summary>The provider's id for the event; the same event delivered twice has the same id.</summary>
    public string EventId { get; init; } = string.Empty;

    public string? EventType { get; init; }

    /// <summary>The raw request body as UTF-8 text, exactly as it was signed.</summary>
    public string Body { get; init; } = string.Empty;

    public string? ContentType { get; init; }

    /// <summary>Provider headers worth keeping (topic, shop, hook ids) plus any added with <see cref="WebhookInboxBuilder.ForwardHeaders"/>.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; init; } = NoHeaders;

    public DateTimeOffset ReceivedAt { get; init; }
}
