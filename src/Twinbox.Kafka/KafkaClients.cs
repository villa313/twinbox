using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Twinbox.Kafka;

/// <summary>Owns the producer and admin client shared by sending and dead-lettering, and builds listener consumers.</summary>
internal sealed partial class KafkaClients(IOptions<KafkaOptions> options, ILogger<KafkaClients> logger) : IDisposable
{
    private readonly ILogger _logger = logger;
    private readonly object _gate = new();
    private readonly ConcurrentDictionary<string, bool> _knownTopics = new(StringComparer.Ordinal);
    private IProducer<string?, byte[]>? _producer;
    private IAdminClient? _admin;
    private bool _disposed;

    public KafkaOptions Options { get; } = options.Value;

    public async Task ProduceAsync(string topic, Message<string?, byte[]> message, CancellationToken cancellationToken)
    {
        var producer = GetProducer();
        try
        {
            await producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (KafkaErrors.IsFatal(ex))
        {
            LogProducerReplaced(ex);
            DiscardProducer(producer);
            throw;
        }
    }

    /// <summary>Creates the topic when <see cref="KafkaOptions.AutoCreateTopics"/> is on; an existing topic counts as success.</summary>
    public async Task EnsureTopicAsync(string topic, CancellationToken cancellationToken)
    {
        if (!Options.AutoCreateTopics || _knownTopics.ContainsKey(topic))
        {
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        var specification = new TopicSpecification
        {
            Name = topic,
            NumPartitions = Options.TopicPartitions,
            ReplicationFactor = Options.TopicReplicationFactor,
        };

        try
        {
            await GetAdmin().CreateTopicsAsync([specification], new CreateTopicsOptions { RequestTimeout = Options.SendTimeout })
                .ConfigureAwait(false);
            LogTopicCreated(topic, Options.TopicPartitions, Options.TopicReplicationFactor);
        }
        catch (CreateTopicsException ex) when (ex.Results.TrueForAll(r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
        {
            // Another instance, or an operator, created it first.
        }

        _knownTopics.TryAdd(topic, true);
    }

    public IConsumer<string?, byte[]> CreateConsumer(
        KafkaListener listener,
        Action<IConsumer<string?, byte[]>, List<TopicPartition>> assigned,
        Action<IConsumer<string?, byte[]>, List<TopicPartitionOffset>> revoked)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new ConsumerBuilder<string?, byte[]>(CreateConsumerConfig(Options, listener))
            .SetLogHandler((_, message) => LogClient(ToLogLevel(message), message.Name, message.Facility, message.Message))
            .SetErrorHandler((_, error) => LogClientError(error.IsFatal ? LogLevel.Error : LogLevel.Warning, error.Code, error.Reason))
            .SetPartitionsAssignedHandler(assigned)
            .SetPartitionsRevokedHandler(revoked)
            .SetPartitionsLostHandler(revoked)
            .Build();
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            _producer?.Dispose();
            _producer = null;
            _admin?.Dispose();
            _admin = null;
        }
    }

    internal static ProducerConfig CreateProducerConfig(KafkaOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = options.ClientId,
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = (int)options.SendTimeout.TotalMilliseconds,
        };
        options.ConfigureClient?.Invoke(config);
        options.ConfigureProducer?.Invoke(config);
        return config;
    }

    internal static ConsumerConfig CreateConsumerConfig(KafkaOptions options, KafkaListener listener)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = options.ClientId,
            GroupId = listener.GroupId,
            AutoOffsetReset = options.AutoOffsetReset,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
        };
        options.ConfigureClient?.Invoke(config);
        options.ConfigureConsumer?.Invoke(config);
        return config;
    }

    private IProducer<string?, byte[]> GetProducer()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _producer ??= new ProducerBuilder<string?, byte[]>(CreateProducerConfig(Options))
                .SetLogHandler((_, message) => LogClient(ToLogLevel(message), message.Name, message.Facility, message.Message))
                .SetErrorHandler((_, error) => LogClientError(error.IsFatal ? LogLevel.Error : LogLevel.Warning, error.Code, error.Reason))
                .Build();
        }
    }

    private IAdminClient GetAdmin()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_admin is null)
            {
                var config = new AdminClientConfig { BootstrapServers = Options.BootstrapServers, ClientId = Options.ClientId };
                Options.ConfigureClient?.Invoke(config);
                _admin = new AdminClientBuilder(config)
                    .SetLogHandler((_, message) => LogClient(ToLogLevel(message), message.Name, message.Facility, message.Message))
                    .Build();
            }

            return _admin;
        }
    }

    private void DiscardProducer(IProducer<string?, byte[]> producer)
    {
        lock (_gate)
        {
            if (ReferenceEquals(_producer, producer))
            {
                _producer = null;
                producer.Dispose();
            }
        }
    }

    private static LogLevel ToLogLevel(LogMessage message) => (LogLevel)message.LevelAs(LogLevelType.MicrosoftExtensionsLogging);

    [LoggerMessage(Message = "Kafka client {Client} [{Facility}]: {Text}")]
    private partial void LogClient(LogLevel level, string client, string facility, string text);

    [LoggerMessage(Message = "Kafka client error {Code}: {Reason}")]
    private partial void LogClientError(LogLevel level, ErrorCode code, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created Kafka topic {Topic} with {Partitions} partition(s) and replication factor {ReplicationFactor}.")]
    private partial void LogTopicCreated(string topic, int partitions, short replicationFactor);

    [LoggerMessage(Level = LogLevel.Error, Message = "The Kafka producer hit a fatal error; replacing it.")]
    private partial void LogProducerReplaced(Exception error);
}
