using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.RabbitMQ;
using Twinbox.Transport;

namespace Twinbox;

public static class RabbitMQTwinboxBuilderExtensions
{
    /// <summary>Connects with an amqp:// or amqps:// URI, e.g. "amqp://user:pass@host:5672/vhost".</summary>
    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UseRabbitMQ(this TwinboxBuilder builder, string connectionString, Action<RabbitMQOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        if (!Uri.TryCreate(connectionString, UriKind.Absolute, out var uri) || uri.Scheme is not ("amqp" or "amqps"))
        {
            throw new ArgumentException("A RabbitMQ connection string must be an absolute amqp:// or amqps:// URI.", nameof(connectionString));
        }

        return builder.UseRabbitMQ(options =>
        {
            options.ConnectionUri = uri;
            configure?.Invoke(options);
        });
    }

    /// <summary>Sends through RabbitMQ and consumes the queues registered with <see cref="RabbitMQOptions.Listen"/>.</summary>
    public static TwinboxBuilder UseRabbitMQ(this TwinboxBuilder builder, Action<RabbitMQOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<RabbitMQOptions>()
            .Configure(configure)
            .Validate(o => o.MaxDeliveryAttempts > 0, "RabbitMQ MaxDeliveryAttempts must be positive.")
            .Validate(o => o.RetryDelay > TimeSpan.Zero, "RabbitMQ RetryDelay must be positive.")
            .Validate(o => o.MaxRetryDelay >= o.RetryDelay && o.MaxRetryDelay.TotalMilliseconds <= int.MaxValue, "RabbitMQ MaxRetryDelay cannot be shorter than RetryDelay.")
            .Validate(o => o.PrefetchCount is > 0 and <= ushort.MaxValue, "RabbitMQ PrefetchCount must be between 1 and 65535.")
            .Validate(o => o.ConsumerConcurrency is > 0 and <= ushort.MaxValue, "RabbitMQ ConsumerConcurrency must be between 1 and 65535.")
            .Validate(o => o.PublishChannelPoolSize > 0, "RabbitMQ PublishChannelPoolSize must be positive.")
            .Validate(o => o.PublishTimeout > TimeSpan.Zero, "RabbitMQ PublishTimeout must be positive.")
            .Validate(o => o.ConnectionUri is not null || !string.IsNullOrWhiteSpace(o.HostName), "RabbitMQ needs a ConnectionUri or a HostName.");

        builder.Services.TryAddSingleton<RabbitMQConnection>();
        builder.Services.TryAddSingleton(sp => new RabbitMQTransport(
            sp.GetRequiredService<RabbitMQConnection>(),
            sp.GetRequiredService<IOptions<RabbitMQOptions>>().Value,
            sp.GetRequiredService<ILogger<RabbitMQTransport>>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, RabbitMQTransport>(sp => sp.GetRequiredService<RabbitMQTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RabbitMQConsumerService>());
        return builder;
    }
}
