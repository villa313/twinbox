using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.RabbitMQ;
using Twinbox.Transport;

namespace Twinbox;

public static class RabbitMqTwinboxBuilderExtensions
{
    /// <summary>Sends through RabbitMQ and consumes the queues registered with <see cref="RabbitMqOptions.Listen"/>.</summary>
    public static TwinboxBuilder UseRabbitMq(this TwinboxBuilder builder, Action<RabbitMqOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<RabbitMqOptions>()
            .Configure(configure)
            .Validate(o => o.DeliveryLimit > 0, "RabbitMQ DeliveryLimit must be positive.")
            .Validate(o => o.PrefetchCount > 0, "RabbitMQ PrefetchCount must be positive.")
            .Validate(o => o.PublishChannelPoolSize > 0, "RabbitMQ PublishChannelPoolSize must be positive.")
            .Validate(o => o.PublishTimeout > TimeSpan.Zero, "RabbitMQ PublishTimeout must be positive.")
            .Validate(o => o.ConnectionUri is not null || !string.IsNullOrWhiteSpace(o.HostName), "RabbitMQ needs a ConnectionUri or a HostName.");

        builder.Services.TryAddSingleton<RabbitMqConnection>();
        builder.Services.TryAddSingleton(sp => new RabbitMqTransport(
            sp.GetRequiredService<RabbitMqConnection>(),
            sp.GetRequiredService<IOptions<RabbitMqOptions>>().Value,
            sp.GetRequiredService<ILogger<RabbitMqTransport>>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, RabbitMqTransport>(sp => sp.GetRequiredService<RabbitMqTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RabbitMqConsumerService>());
        return builder;
    }
}
