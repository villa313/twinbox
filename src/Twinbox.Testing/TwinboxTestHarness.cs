using Twinbox.InMemory;
using Twinbox.Messaging;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Testing;

/// <summary>Runs dispatch and delivery on demand so tests are deterministic, with no background timing.</summary>
public sealed class TwinboxTestHarness
{
    private const int MaxRounds = 1000;

    private readonly IOutboxDispatcher _dispatcher;
    private readonly IInboundPipeline _pipeline;
    private readonly MessageTypeRegistry _registry;
    private readonly IMessageSerializer _serializer;

    internal TwinboxTestHarness(
        IOutboxDispatcher dispatcher,
        IInboundPipeline pipeline,
        InMemoryOutboxStore store,
        InMemoryTransport transport,
        MessageTypeRegistry registry,
        IMessageSerializer serializer)
    {
        _dispatcher = dispatcher;
        _pipeline = pipeline;
        _registry = registry;
        _serializer = serializer;
        Store = store;
        Transport = transport;
    }

    public InMemoryOutboxStore Store { get; }

    public InMemoryTransport Transport { get; }

    /// <summary>Dispatches and delivers until nothing due is left, including messages that handlers send.</summary>
    public async Task DrainAsync(CancellationToken cancellationToken = default)
    {
        for (var round = 0; round < MaxRounds; round++)
        {
            var dispatched = await _dispatcher.DispatchBatchAsync(cancellationToken).ConfigureAwait(false);
            var delivered = await Transport.DeliverAsync(_pipeline, cancellationToken).ConfigureAwait(false);
            if (dispatched == 0 && delivered == 0)
            {
                return;
            }
        }

        throw new InvalidOperationException($"Messaging did not settle after {MaxRounds} rounds; a handler may be sending in a loop.");
    }

    public IReadOnlyList<TMessage> Sent<TMessage>()
        where TMessage : class
    {
        var name = _registry.GetOrAdd(typeof(TMessage));
        return [.. Transport.Sent
            .Where(m => m.MessageName == name)
            .Select(m => (TMessage)_serializer.Deserialize(m.Body.Span, typeof(TMessage)))];
    }

    public IReadOnlyList<OutboxMessage> DeadLettered() =>
        [.. Store.Snapshot().Where(m => m.Status == OutboxMessageStatus.Dead)];
}
