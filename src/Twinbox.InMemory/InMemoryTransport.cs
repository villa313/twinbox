using System.Collections.Concurrent;
using Twinbox.Transport;

namespace Twinbox.InMemory;

/// <summary>Queues messages in memory and delivers them to this app's own handlers, like a loopback broker.</summary>
public sealed class InMemoryTransport : ITransport, IDisposable
{
    public const string DefaultName = "inmemory";

    private const int MaxDeliveryAttempts = 10;

    private readonly ConcurrentQueue<(TransportMessage Message, int Attempt)> _queue = new();
    private readonly ConcurrentQueue<TransportMessage> _sent = new();
    private readonly ConcurrentQueue<TransportMessage> _deadLettered = new();
    private readonly SemaphoreSlim _available = new(0);

    public string Name => DefaultName;

    /// <summary>Runs before each send; throw from it to simulate a broker failure.</summary>
    public Func<TransportMessage, Task>? OnSend { get; set; }

    public IReadOnlyList<TransportMessage> Sent => [.. _sent];

    public IReadOnlyList<TransportMessage> DeadLettered => [.. _deadLettered];

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (OnSend is { } onSend)
        {
            await onSend(message).ConfigureAwait(false);
        }

        _sent.Enqueue(message);
        _queue.Enqueue((message, 1));
        _available.Release();
    }

    /// <summary>Delivers everything queued right now; returns how many deliveries were attempted.</summary>
    public async Task<int> DeliverAsync(IInboundPipeline pipeline, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        var count = _queue.Count;
        var delivered = 0;
        for (var i = 0; i < count && _queue.TryDequeue(out var item); i++)
        {
            delivered++;
            try
            {
                await pipeline.ProcessAsync(ToIncoming(item.Message, item.Attempt), cancellationToken).ConfigureAwait(false);
            }
            catch (PermanentDeliveryException)
            {
                _deadLettered.Enqueue(item.Message);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                if (item.Attempt >= MaxDeliveryAttempts)
                {
                    _deadLettered.Enqueue(item.Message);
                }
                else
                {
                    _queue.Enqueue((item.Message, item.Attempt + 1));
                }
            }
        }

        return delivered;
    }

    public Task WaitForMessagesAsync(TimeSpan timeout, CancellationToken cancellationToken) =>
        _available.WaitAsync(timeout, cancellationToken);

    public void Dispose() => _available.Dispose();

    private static IncomingMessage ToIncoming(TransportMessage message, int attempt) => new(
        message.MessageId,
        message.MessageName,
        message.Destination,
        message.Body,
        message.ContentType,
        message.Headers,
        attempt,
        message.PartitionKey);
}
