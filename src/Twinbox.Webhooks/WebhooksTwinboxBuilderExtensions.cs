using Microsoft.Extensions.DependencyInjection.Extensions;
using Twinbox.Webhooks;

namespace Twinbox;

public static class WebhooksTwinboxBuilderExtensions
{
    /// <summary>Enables <c>MapWebhookInbox</c>. Webhooks are delivered to your handlers through local delivery, which this turns on.</summary>
    public static TwinboxBuilder AddWebhooks(this TwinboxBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.UseLocalDelivery();
        builder.Services.TryAddSingleton<WebhookIngress>();
        return builder;
    }
}
