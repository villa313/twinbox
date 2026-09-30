using System.Diagnostics.CodeAnalysis;
using DotPulsar;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.Pulsar;
using Twinbox.Transport;

namespace Twinbox;

public static class PulsarTwinboxBuilderExtensions
{
    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UsePulsar(this TwinboxBuilder builder, string serviceUrl, Action<PulsarOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceUrl);
        var uri = new Uri(serviceUrl, UriKind.Absolute);
        return builder.UsePulsar(options =>
        {
            options.ServiceUrl = uri;
            configure?.Invoke(options);
        });
    }

    /// <summary>Sends through Pulsar and consumes the topics registered with <see cref="PulsarOptions.Listen"/>.</summary>
    public static TwinboxBuilder UsePulsar(this TwinboxBuilder builder, Action<PulsarOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<PulsarOptions>()
            .Configure(configure)
            .Validate(o => o.ServiceUrl is { IsAbsoluteUri: true, Scheme: "pulsar" or "pulsar+ssl" }, "Pulsar ServiceUrl must be an absolute pulsar:// or pulsar+ssl:// address.")
            .Validate(o => Enum.IsDefined(o.SubscriptionType), "Pulsar SubscriptionType is not a known subscription type.")
            .Validate(o => Enum.IsDefined(o.InitialPosition), "Pulsar InitialPosition is not a known position.")
            .Validate(o => o.MaxDeliveryAttempts > 0, "Pulsar MaxDeliveryAttempts must be positive.")
            .Validate(o => IsDelay(o.RetryDelay, allowZero: false), "Pulsar RetryDelay must be positive.")
            .Validate(o => IsDelay(o.MaxRetryDelay, allowZero: false) && o.MaxRetryDelay >= o.RetryDelay, "Pulsar MaxRetryDelay must be between RetryDelay and about 24 days.")
            .Validate(o => !string.IsNullOrWhiteSpace(o.DeadLetterSuffix), "Pulsar DeadLetterSuffix is required.")
            .Validate(o => o.ConsumerConcurrency > 0, "Pulsar ConsumerConcurrency must be positive.")
            .Validate(
                o => o.ConsumerConcurrency == 1 || o.SubscriptionType is SubscriptionType.Shared or SubscriptionType.KeyShared,
                "Pulsar ConsumerConcurrency above 1 needs a Shared or KeyShared subscription.")
            .Validate(o => IsDelay(o.SendTimeout, allowZero: false), "Pulsar SendTimeout must be positive.");

        builder.Services.TryAddSingleton<PulsarClients>();
        builder.Services.TryAddSingleton(sp => new PulsarTransport(sp.GetRequiredService<PulsarClients>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, PulsarTransport>(sp => sp.GetRequiredService<PulsarTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, PulsarConsumerService>());
        return builder;
    }

    private static bool IsDelay(TimeSpan value, bool allowZero) =>
        (allowZero ? value >= TimeSpan.Zero : value > TimeSpan.Zero) && value.TotalMilliseconds <= int.MaxValue;
}
