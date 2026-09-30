using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Inbox;
using Twinbox.Storage;
using Twinbox.Transport;
using Twinbox.Tenancy;

namespace Twinbox.Messaging;

internal sealed partial class OutboxBuffer(
    MessagePreparer preparer,
    IServiceProvider services,
    ILogger<OutboxBuffer> logger,
    TenancyOptions? tenancy = null)
    : IOutbox, IOutboxSession, IDisposable
{
    private static readonly SendOptions DefaultOptions = new();

    private readonly ILogger _logger = logger;

    private readonly List<OutboxMessage> _pending = [];
    private readonly object _gate = new();

    public IServiceProvider Services => services;

    public bool HasPending
    {
        get
        {
            lock (_gate)
            {
                return _pending.Count > 0;
            }
        }
    }

    public void Send<TMessage>(TMessage message, SendOptions? options = null)
        where TMessage : class =>
        Send(message, options, physicalDestination: false);

    /// <summary>Sends to <see cref="SendOptions.Destination"/> as given, without the destination prefix.</summary>
    public void Send<TMessage>(TMessage message, SendOptions? options, bool physicalDestination)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        options ??= DefaultOptions;
        // Inside a handler the inbound tenant wins; otherwise ask the app which tenant this scope belongs to.
        var tenant = TenantScope.Current ?? tenancy?.CurrentTenant?.Invoke(services);
        var headers = OutgoingHeaders(message, options);
        var prepared = preparer.Prepare(message, headers is null ? options : options with { Headers = headers }, tenant, physicalDestination);
        lock (_gate)
        {
            _pending.AddRange(prepared);
        }
    }

    public IReadOnlyList<OutboxMessage> TakePending()
    {
        lock (_gate)
        {
            var taken = _pending.ToArray();
            _pending.Clear();
            return taken;
        }
    }

    public void Dispose()
    {
        int count;
        lock (_gate)
        {
            count = _pending.Count;
        }

        if (count > 0)
        {
            LogUnsavedMessages(count);
        }
    }

    /// <summary>Returns null when the message carries no headers, sparing a dictionary per send.</summary>
    private Dictionary<string, string>? OutgoingHeaders(object message, SendOptions options)
    {
        var inbound = InboundContext.Current;
        var correlationId = options.CorrelationId ?? inbound?.CorrelationId ?? inbound?.MessageId;
        var filters = services.GetServices<IOutgoingMessageFilter>();
        if (options.Headers is null && correlationId is null && options.ReplyTo is null && filters is IOutgoingMessageFilter[] { Length: 0 })
        {
            return null;
        }

        var headers = options.Headers is null ? [] : new Dictionary<string, string>(options.Headers);
        if (correlationId is not null)
        {
            headers[TransportHeaders.CorrelationId] = correlationId;
        }

        if (options.ReplyTo is not null)
        {
            headers[TransportHeaders.ReplyTo] = options.ReplyTo;
        }

        foreach (var filter in filters)
        {
            filter.OnSending(message, headers);
        }

        return headers;
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "{Count} outbox message(s) were sent but never saved. Save a Twinbox-enabled DbContext resolved in the same scope, call context.EnlistOutbox(outbox) for contexts you create yourself, or use outbox.CommitAsync(transaction or session) with ADO.NET or MongoDB.")]
    private partial void LogUnsavedMessages(int count);
}
