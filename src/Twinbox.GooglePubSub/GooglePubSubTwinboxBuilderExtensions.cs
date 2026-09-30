using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.GooglePubSub;
using Twinbox.Transport;

namespace Twinbox;

public static class GooglePubSubTwinboxBuilderExtensions
{
    private static readonly TimeSpan MinAckDeadline = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan MaxAckDeadline = TimeSpan.FromSeconds(600);

    public static TwinboxBuilder UseGooglePubSub(this TwinboxBuilder builder, string projectId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        return builder.UseGooglePubSub(options => options.ProjectId = projectId);
    }

    /// <summary>Publishes to Pub/Sub topics and consumes the subscriptions registered with Subscribe.</summary>
    public static TwinboxBuilder UseGooglePubSub(this TwinboxBuilder builder, Action<GooglePubSubOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<GooglePubSubOptions>()
            .Configure(configure)
            .Validate(o => !string.IsNullOrWhiteSpace(o.ProjectId), "Google Pub/Sub ProjectId is required.")
            .Validate(o => o.EmulatorHost is null || !string.IsNullOrWhiteSpace(o.EmulatorHost), "Google Pub/Sub EmulatorHost cannot be blank.")
            .Validate(o => o.AckDeadline >= MinAckDeadline && o.AckDeadline <= MaxAckDeadline, "Google Pub/Sub AckDeadline must be between 10 and 600 seconds.")
            .Validate(o => o.MaxOutstandingMessages > 0, "Google Pub/Sub MaxOutstandingMessages must be positive.")
            .Validate(o => o.MaxDeliveryAttempts is >= 5 and <= 100, "Google Pub/Sub MaxDeliveryAttempts must be between 5 and 100.")
            .Validate(o => o.DeadLetterTopic is null || !string.IsNullOrWhiteSpace(o.DeadLetterTopic), "Google Pub/Sub DeadLetterTopic cannot be blank.");

        builder.Services.TryAddSingleton<GooglePubSubClients>();
        builder.Services.TryAddSingleton(sp => new GooglePubSubTransport(sp.GetRequiredService<GooglePubSubClients>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, GooglePubSubTransport>(sp => sp.GetRequiredService<GooglePubSubTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, GooglePubSubSubscriberService>());
        return builder;
    }
}
