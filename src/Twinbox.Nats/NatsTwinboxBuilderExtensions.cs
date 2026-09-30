using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.Nats;
using Twinbox.Transport;

namespace Twinbox;

public static class NatsTwinboxBuilderExtensions
{
    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UseNats(this TwinboxBuilder builder, string url, Action<NatsOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(url);
        return builder.UseNats(options =>
        {
            options.Url = url;
            configure?.Invoke(options);
        });
    }

    /// <summary>Sends through NATS JetStream and consumes the streams registered with <see cref="NatsOptions.Listen"/>.</summary>
    public static TwinboxBuilder UseNats(this TwinboxBuilder builder, Action<NatsOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<NatsOptions>()
            .Configure(configure)
            .Validate(o => !string.IsNullOrWhiteSpace(o.Url), "NATS Url is required.")
            .Validate(o => o.AckWait > TimeSpan.Zero, "NATS AckWait must be positive.")
            .Validate(o => o.MaxDeliveryAttempts > 0, "NATS MaxDeliveryAttempts must be positive.")
            .Validate(o => o.MaxAckPending > 0, "NATS MaxAckPending must be positive.")
            .Validate(o => o.PrefetchCount > 0, "NATS PrefetchCount must be positive.")
            .Validate(o => o.ConsumerConcurrency > 0, "NATS ConsumerConcurrency must be positive.")
            .Validate(o => o.DuplicateWindow > TimeSpan.Zero, "NATS DuplicateWindow must be positive.")
            .Validate(o => o.RetryDelay > TimeSpan.Zero, "NATS RetryDelay must be positive.")
            .Validate(o => o.MaxRetryDelay >= o.RetryDelay, "NATS MaxRetryDelay cannot be shorter than RetryDelay.")
            .Validate(o => o.DeadLetterSubject is null || !string.IsNullOrWhiteSpace(o.DeadLetterSubject), "NATS DeadLetterSubject cannot be blank.");

        builder.Services.TryAddSingleton<NatsClients>();
        builder.Services.TryAddSingleton(sp => new NatsTransport(sp.GetRequiredService<NatsClients>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, NatsTransport>(sp => sp.GetRequiredService<NatsTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, NatsConsumerService>());
        return builder;
    }
}
