using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Twinbox.Nats;

/// <summary>Owns the connection shared by sending, dead-lettering and listening, and the streams created on it.</summary>
internal sealed partial class NatsClients(IOptions<NatsOptions> options, ILoggerFactory loggerFactory, ILogger<NatsClients> logger)
    : IAsyncDisposable
{
    private readonly ILogger _logger = logger;
    private readonly SemaphoreSlim _streamsGate = new(1, 1);
    private readonly Lazy<NatsConnection> _connection = new(() => new NatsConnection(CreateConnectionOptions(options.Value, loggerFactory)));
    private NatsJSContext? _jetStream;
    private volatile bool _streamsEnsured;

    public NatsOptions Options { get; } = options.Value;

    public NatsJSContext JetStream => LazyInitializer.EnsureInitialized(ref _jetStream, () => new NatsJSContext(_connection.Value));

    public async Task<PubAckResponse> PublishAsync(string subject, ReadOnlyMemory<byte> body, NatsHeaders headers, CancellationToken cancellationToken)
    {
        await EnsureStreamsAsync(cancellationToken).ConfigureAwait(false);
        return await JetStream.PublishAsync(
            subject,
            body,
            NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
            headers: headers,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>True only when JetStream answered and no stream captures <paramref name="subject"/>.</summary>
    public async Task<bool> IsUnroutableAsync(string subject, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var _ in JetStream.ListStreamNamesAsync(subject, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }

            return true;
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            LogStreamLookupFailed(ex, subject);
            return false;
        }
    }

    public async Task<INatsJSConsumer> CreateConsumerAsync(NatsListener listener, CancellationToken cancellationToken) =>
        await JetStream.CreateOrUpdateConsumerAsync(listener.Stream, CreateConsumerConfig(Options, listener), cancellationToken)
            .ConfigureAwait(false);

    /// <summary>Creates the configured streams once when <see cref="NatsOptions.AutoCreateStreams"/> is on.</summary>
    public async Task EnsureStreamsAsync(CancellationToken cancellationToken)
    {
        if (!Options.AutoCreateStreams || _streamsEnsured)
        {
            return;
        }

        await _streamsGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_streamsEnsured)
            {
                return;
            }

            foreach (var stream in Options.Streams)
            {
                await CreateStreamAsync(stream, cancellationToken).ConfigureAwait(false);
            }

            _streamsEnsured = true;
        }
        finally
        {
            _streamsGate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection.IsValueCreated)
        {
            await _connection.Value.DisposeAsync().ConfigureAwait(false);
        }

        _streamsGate.Dispose();
    }

    internal static NatsOpts CreateConnectionOptions(NatsOptions options, ILoggerFactory loggerFactory)
    {
        var opts = NatsOpts.Default with
        {
            Url = options.Url,
            Name = options.ClientName,
            LoggerFactory = loggerFactory,
        };
        return options.ConfigureConnection?.Invoke(opts) ?? opts;
    }

    internal static ConsumerConfig CreateConsumerConfig(NatsOptions options, NatsListener listener) => new(listener.DurableConsumer)
    {
        DurableName = listener.DurableConsumer,
        AckPolicy = ConsumerConfigAckPolicy.Explicit,
        AckWait = options.AckWait,
        MaxDeliver = options.MaxDeliver,
        MaxAckPending = options.MaxAckPending,
        FilterSubject = listener.FilterSubject,
    };

    internal static StreamConfig CreateStreamConfig(NatsOptions options, NatsStream stream) => new(stream.Name, [.. stream.Subjects])
    {
        DuplicateWindow = options.DuplicateWindow,
    };

    private async Task CreateStreamAsync(NatsStream stream, CancellationToken cancellationToken)
    {
        try
        {
            await JetStream.CreateStreamAsync(CreateStreamConfig(Options, stream), cancellationToken).ConfigureAwait(false);
            LogStreamCreated(stream.Name, string.Join(", ", stream.Subjects));
        }
        catch (NatsJSApiException ex) when (ex.Error.ErrCode == NatsErrors.StreamNameInUse)
        {
            // It exists with other settings, e.g. tuned by an operator; theirs win.
            LogStreamKept(stream.Name);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created NATS stream {Stream} for subjects [{Subjects}].")]
    private partial void LogStreamCreated(string stream, string subjects);

    [LoggerMessage(Level = LogLevel.Debug, Message = "NATS stream {Stream} already exists with other settings; keeping it as is.")]
    private partial void LogStreamKept(string stream);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Could not look up which NATS stream captures {Subject}.")]
    private partial void LogStreamLookupFailed(Exception error, string subject);
}
