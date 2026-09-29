using System.Buffers;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Exceptions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Twinbox.Pulsar;

/// <summary>Owns the client and the per-topic producers shared by sending and dead-lettering, and builds listener consumers.</summary>
internal sealed partial class PulsarClients(IOptions<PulsarOptions> options, ILogger<PulsarClients> logger) : IAsyncDisposable, IDisposable
{
    private readonly ILogger _logger = logger;
    private readonly object _gate = new();
    private readonly Dictionary<string, IProducer<ReadOnlySequence<byte>>> _producers = new(StringComparer.Ordinal);
    private IPulsarClient? _client;
    private bool _disposed;

    public PulsarOptions Options { get; } = options.Value;

    public async Task SendAsync(string topic, MessageMetadata metadata, ReadOnlySequence<byte> body, CancellationToken cancellationToken)
    {
        var producer = GetProducer(topic);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(Options.SendTimeout);
        try
        {
            await producer.Send(metadata, body, timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException ex) when (timeout.IsCancellationRequested)
        {
            throw new TimeoutException($"Pulsar did not confirm the send to '{topic}' within {Options.SendTimeout}.", ex);
        }
        catch (OperationCanceledException ex)
        {
            // A faulting producer cancels its pending sends; only a later send learns why.
            var fault = await FaultOfAsync(producer).ConfigureAwait(false);
            await DiscardAsync(topic, producer, fault ?? ex).ConfigureAwait(false);
            if (fault is null)
            {
                throw;
            }

            throw fault;
        }
        catch (Exception ex) when (producer.State.IsFinalState())
        {
            await DiscardAsync(topic, producer, ex).ConfigureAwait(false);
            throw;
        }
    }

    public IConsumer<ReadOnlySequence<byte>> CreateConsumer(PulsarListener listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        return GetClient().NewConsumer()
            .Topic(listener.Topic)
            .SubscriptionName(listener.Subscription)
            .SubscriptionType(Options.SubscriptionType)
            .InitialPosition(Options.InitialPosition)
            .Create();
    }

    /// <summary>Lets a service provider that is disposed synchronously still close the client.</summary>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        IProducer<ReadOnlySequence<byte>>[] producers;
        IPulsarClient? client;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            producers = [.. _producers.Values];
            _producers.Clear();
            client = _client;
            _client = null;
        }

        foreach (var producer in producers)
        {
            await producer.DisposeAsync().ConfigureAwait(false);
        }

        if (client is not null)
        {
            await client.DisposeAsync().ConfigureAwait(false);
        }
    }

    internal IPulsarClientBuilder CreateClientBuilder()
    {
        var builder = PulsarClient.Builder().ServiceUrl(Options.ServiceUrl!);
        Options.ConfigureClient?.Invoke(builder);
        return builder;
    }

    // Sending a message with an already cancelled token reaches DotPulsar's fault check but is never written to the broker.
    private static async Task<Exception?> FaultOfAsync(IProducer<ReadOnlySequence<byte>> producer)
    {
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync().ConfigureAwait(false);
        try
        {
            await producer.Send(new MessageMetadata(), ReadOnlySequence<byte>.Empty, cancelled.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is FaultedException or ProducerFencedException)
        {
            return ex;
        }
        catch (Exception)
        {
            // Cancelled or closed: nothing more to learn.
        }

        return null;
    }

    private IPulsarClient GetClient()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_client is null)
            {
                _client = CreateClientBuilder().Build();
                LogClientCreated(Options.ServiceUrl!);
            }

            return _client;
        }
    }

    private IProducer<ReadOnlySequence<byte>> GetProducer(string topic)
    {
        var client = GetClient();
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_producers.TryGetValue(topic, out var producer))
            {
                producer = client.NewProducer().Topic(topic).Create();
                _producers[topic] = producer;
            }

            return producer;
        }
    }

    private async Task DiscardAsync(string topic, IProducer<ReadOnlySequence<byte>> producer, Exception cause)
    {
        lock (_gate)
        {
            if (!_producers.TryGetValue(topic, out var current) || !ReferenceEquals(current, producer))
            {
                return;
            }

            _producers.Remove(topic);
        }

        LogProducerReplaced(cause, topic);
        await producer.DisposeAsync().ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created Pulsar client for {ServiceUrl}.")]
    private partial void LogClientCreated(Uri serviceUrl);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The Pulsar producer for {Topic} failed; replacing it.")]
    private partial void LogProducerReplaced(Exception error, string topic);
}
