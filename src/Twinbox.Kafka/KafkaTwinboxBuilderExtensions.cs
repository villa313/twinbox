using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.Kafka;
using Twinbox.Transport;

namespace Twinbox;

public static class KafkaTwinboxBuilderExtensions
{
    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UseKafka(this TwinboxBuilder builder, string bootstrapServers, Action<KafkaOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bootstrapServers);
        return builder.UseKafka(options =>
        {
            options.BootstrapServers = bootstrapServers;
            configure?.Invoke(options);
        });
    }

    /// <summary>Sends through Kafka and consumes the topics registered with <see cref="KafkaOptions.Listen"/>.</summary>
    public static TwinboxBuilder UseKafka(this TwinboxBuilder builder, Action<KafkaOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<KafkaOptions>()
            .Configure(configure)
            .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "Kafka BootstrapServers is required.")
            .Validate(o => o.TopicPartitions > 0, "Kafka TopicPartitions must be positive.")
            .Validate(o => o.TopicReplicationFactor > 0, "Kafka TopicReplicationFactor must be positive.")
            .Validate(o => o.SendTimeout > TimeSpan.Zero && o.SendTimeout.TotalMilliseconds <= int.MaxValue, "Kafka SendTimeout must be positive.")
            .Validate(o => o.RetryDelay > TimeSpan.Zero, "Kafka RetryDelay must be positive.")
            .Validate(o => o.MaxRetryDelay >= o.RetryDelay, "Kafka MaxRetryDelay cannot be shorter than RetryDelay.")
            .Validate(o => o.MaxBatchSize > 0, "Kafka MaxBatchSize must be positive.")
            .Validate(o => o.MaxBatchWait >= TimeSpan.Zero && o.MaxBatchWait.TotalMilliseconds <= int.MaxValue, "Kafka MaxBatchWait cannot be negative.")
            .Validate(o => o.DeadLetterTopic is null || !string.IsNullOrWhiteSpace(o.DeadLetterTopic), "Kafka DeadLetterTopic cannot be blank.");

        builder.Services.TryAddSingleton<KafkaClients>();
        builder.Services.TryAddSingleton(sp => new KafkaTransport(sp.GetRequiredService<KafkaClients>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, KafkaTransport>(sp => sp.GetRequiredService<KafkaTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, KafkaConsumerService>());
        return builder;
    }
}
