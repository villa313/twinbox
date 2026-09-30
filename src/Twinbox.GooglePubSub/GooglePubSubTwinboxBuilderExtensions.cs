using System.Diagnostics.CodeAnalysis;
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

    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UseGooglePubSub(this TwinboxBuilder builder, string projectId, Action<GooglePubSubOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(projectId);
        return builder.UseGooglePubSub(options =>
        {
            options.ProjectId = projectId;
            configure?.Invoke(options);
        });
    }

    /// <summary>Publishes to Pub/Sub topics and consumes the subscriptions registered with <see cref="GooglePubSubOptions.Listen"/>.</summary>
    public static TwinboxBuilder UseGooglePubSub(this TwinboxBuilder builder, Action<GooglePubSubOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<GooglePubSubOptions>()
            .Configure(configure)
            .Validate(o => o.ProjectId is null || !string.IsNullOrWhiteSpace(o.ProjectId), "Google Pub/Sub ProjectId cannot be blank.")
            .Validate(o => o.ProjectId is not null || !UsesShortNames(o), "Google Pub/Sub ProjectId is required for short topic and subscription names.")
            .Validate(o => o.EmulatorHost is null || !string.IsNullOrWhiteSpace(o.EmulatorHost), "Google Pub/Sub EmulatorHost cannot be blank.")
            .Validate(o => o.AckDeadline >= MinAckDeadline && o.AckDeadline <= MaxAckDeadline, "Google Pub/Sub AckDeadline must be between 10 and 600 seconds.")
            .Validate(o => o.MaxOutstandingMessages > 0, "Google Pub/Sub MaxOutstandingMessages must be positive.")
            .Validate(
                o => !AppliesDeadLetterPolicy(o) || o.MaxDeliveryAttempts is >= 5 and <= 100,
                "Google Pub/Sub MaxDeliveryAttempts must be between 5 and 100.")
            .Validate(o => o.DeadLetterTopic is null || !string.IsNullOrWhiteSpace(o.DeadLetterTopic), "Google Pub/Sub DeadLetterTopic cannot be blank.");

        builder.Services.TryAddSingleton<GooglePubSubClients>();
        builder.Services.TryAddSingleton(sp => new GooglePubSubTransport(sp.GetRequiredService<GooglePubSubClients>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, GooglePubSubTransport>(sp => sp.GetRequiredService<GooglePubSubTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, GooglePubSubSubscriberService>());
        return builder;
    }

    private static bool UsesShortNames(GooglePubSubOptions options) =>
        (options.DeadLetterTopic is { } deadLetterTopic && !GooglePubSubMapping.IsFullName(deadLetterTopic))
        || options.Subscriptions.Any(s => !GooglePubSubMapping.IsFullName(s.Subscription) || (options.AutoCreate && !GooglePubSubMapping.IsFullName(s.Topic)));

    // Only subscriptions Twinbox creates get a dead-letter policy, so only then does the limit reach Pub/Sub.
    private static bool AppliesDeadLetterPolicy(GooglePubSubOptions options) =>
        options.AutoCreate && options.DeadLetterTopic is not null && options.Subscriptions.Count > 0;
}
