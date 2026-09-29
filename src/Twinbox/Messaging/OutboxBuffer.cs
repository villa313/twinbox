using Microsoft.Extensions.Logging;
using Twinbox.Storage;

namespace Twinbox.Messaging;

internal sealed partial class OutboxBuffer(MessagePreparer preparer, ILogger<OutboxBuffer> logger)
    : IOutbox, IOutboxSession, IDisposable
{
    private readonly ILogger _logger = logger;

    private readonly List<OutboxMessage> _pending = [];
    private readonly object _gate = new();

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
        var prepared = preparer.Prepare(message, options);
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

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Count} outbox message(s) were sent but never saved; the unit of work ended without committing them.")]
    private partial void LogUnsavedMessages(int count);
}
