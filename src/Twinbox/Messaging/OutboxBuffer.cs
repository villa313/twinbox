using Microsoft.Extensions.Logging;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.Messaging;

internal sealed partial class OutboxBuffer(
    MessagePreparer preparer,
    IServiceProvider services,
    ILogger<OutboxBuffer> logger,
    TenancyOptions? tenancy = null)
    : IOutbox, IOutboxSession, IDisposable
{
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
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        // Inside a handler the inbound tenant wins; otherwise ask the app which tenant this scope belongs to.
        var tenant = TenantScope.Current ?? tenancy?.CurrentTenant?.Invoke(services);
        var prepared = preparer.Prepare(message, options, tenant);
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

    [LoggerMessage(Level = LogLevel.Error, Message = "{Count} outbox message(s) were sent but never saved. Save a Twinbox-enabled DbContext resolved in the same scope, call context.EnlistOutbox(outbox) for contexts you create yourself, or use outbox.CommitAsync(transaction) with ADO.NET.")]
    private partial void LogUnsavedMessages(int count);
}
