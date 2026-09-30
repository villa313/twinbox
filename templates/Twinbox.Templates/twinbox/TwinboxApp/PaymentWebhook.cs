using Twinbox;
using Twinbox.Webhooks;

namespace TwinboxApp;

public sealed record PaymentWebhook : WebhookReceived;

public sealed class PaymentWebhookHandler(ILogger<PaymentWebhookHandler> logger) : IHandle<PaymentWebhook>
{
    public Task HandleAsync(PaymentWebhook webhook, MessageContext context, CancellationToken cancellationToken)
    {
        // The signature was verified and the webhook stored before the provider got its 200; this runs once per event id.
        logger.LogInformation("Webhook {EventId} of type {EventType} received", webhook.EventId, webhook.EventType);
        return Task.CompletedTask;
    }
}
