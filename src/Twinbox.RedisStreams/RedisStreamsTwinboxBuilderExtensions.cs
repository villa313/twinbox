using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.RedisStreams;
using Twinbox.Transport;

namespace Twinbox;

public static class RedisStreamsTwinboxBuilderExtensions
{
    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UseRedisStreams(this TwinboxBuilder builder, string configuration, Action<RedisStreamsOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configuration);
        return builder.UseRedisStreams(options =>
        {
            options.Configuration = configuration;
            configure?.Invoke(options);
        });
    }

    /// <summary>Sends through Redis Streams and consumes the streams registered with <see cref="RedisStreamsOptions.Listen"/>.</summary>
    public static TwinboxBuilder UseRedisStreams(this TwinboxBuilder builder, Action<RedisStreamsOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<RedisStreamsOptions>()
            .Configure(configure)
            .Validate(o => o.ConnectionFactory is not null || !string.IsNullOrWhiteSpace(o.Configuration), "Redis Streams needs a Configuration or a ConnectionFactory.")
            .Validate(o => o.MaxLength is null or > 0, "Redis Streams MaxLength must be positive.")
            .Validate(o => o.ClaimIdleAfter >= TimeSpan.FromMilliseconds(1), "Redis Streams ClaimIdleAfter must be at least a millisecond.")
            .Validate(o => o.MaxDeliveryAttempts > 0, "Redis Streams MaxDeliveryAttempts must be positive.")
            .Validate(o => o.BatchSize > 0, "Redis Streams BatchSize must be positive.")
            .Validate(o => o.PollInterval > TimeSpan.Zero, "Redis Streams PollInterval must be positive.")
            .Validate(o => !string.IsNullOrWhiteSpace(o.ConsumerName), "Redis Streams ConsumerName is required.")
            .Validate(o => o.RemoveIdleConsumersAfter is null || o.RemoveIdleConsumersAfter >= o.ClaimIdleAfter, "Redis Streams RemoveIdleConsumersAfter cannot be shorter than ClaimIdleAfter.");

        builder.Services.TryAddSingleton<RedisStreamsConnection>();
        builder.Services.TryAddSingleton(sp => new RedisStreamsTransport(sp.GetRequiredService<RedisStreamsConnection>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, RedisStreamsTransport>(sp => sp.GetRequiredService<RedisStreamsTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RedisStreamsConsumerService>());
        return builder;
    }
}
