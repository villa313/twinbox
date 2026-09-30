using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Diagnostics;
using Twinbox.Messaging;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Tenancy;
using Twinbox.Transport;

namespace Twinbox.Inbox;

internal sealed partial class InboundPipeline(
    MessageTypeRegistry registry,
    HandlerRegistry handlers,
    HeaderProfiles headerProfiles,
    IMessageSerializer serializer,
    TwinboxScopeFactory scopeFactory,
    IOptions<TwinboxOptions> options,
    TimeProvider time,
    ILogger<InboundPipeline> logger,
    IInboxStore? inbox = null) : IInboundPipeline
{
    private readonly ILogger _logger = logger;

    public async Task ProcessAsync(IncomingMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        using var activity = StartActivity(message);
        if (Prepare(message) is not { } prepared)
        {
            return;
        }

        using var _ = TenantScope.Enter(prepared.Tenant);
        foreach (var handler in prepared.Handlers)
        {
            await InvokeAsync(handler, prepared.Message, prepared.Body, prepared.Context, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task ProcessBatchAsync(IReadOnlyList<IncomingMessage> messages, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        var prepared = new List<PreparedMessage>(messages.Count);
        foreach (var message in messages)
        {
            if (Prepare(message) is { } item)
            {
                prepared.Add(item);
            }
        }

        // Grouped by type and tenant so each batch handler call stays within one tenant's database.
        foreach (var group in prepared.GroupBy(p => (p.MessageType, p.Tenant)))
        {
            var items = group.ToArray();
            using var _ = TenantScope.Enter(group.Key.Tenant);
            foreach (var handler in items[0].Handlers)
            {
                if (handler is BatchHandlerDescriptor batchHandler)
                {
                    await InvokeBatchAsync(batchHandler, items, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                foreach (var item in items)
                {
                    await InvokeAsync(handler, item.Message, item.Body, item.Context, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    /// <summary>Returns null for a message that should be acknowledged without handling (ignored unknown messages).</summary>
    private PreparedMessage? Prepare(IncomingMessage message)
    {
        message = headerProfiles.Read(message);
        if (string.IsNullOrEmpty(message.MessageId))
        {
            throw new PermanentDeliveryException($"A message from {message.Source} has no message id, so it can't be deduplicated.");
        }

        if (!registry.TryResolve(message.MessageName, out var messageType))
        {
            HandleUnknown(message, "no message type is registered");
            return null;
        }

        var messageHandlers = handlers.For(messageType);
        if (messageHandlers.Count == 0)
        {
            // Acknowledging a message nobody handled would look like a successful delivery.
            HandleUnknown(message, "no handler is registered");
            return null;
        }

        var context = new MessageContext(
            message.MessageId, message.MessageName, message.Source, message.Headers, message.DeliveryAttempt, message.PartitionKey)
        {
            CorrelationId = message.Headers.GetValueOrDefault(TransportHeaders.CorrelationId),
            ReplyTo = message.Headers.GetValueOrDefault(TransportHeaders.ReplyTo),
        };

        return new PreparedMessage(
            message,
            messageType,
            Deserialize(message, messageType),
            context,
            message.Headers.GetValueOrDefault(TransportHeaders.TenantId),
            messageHandlers);
    }

    private static Activity? StartActivity(IncomingMessage message)
    {
        message.Headers.TryGetValue(TransportHeaders.TraceParent, out var traceParent);
        var activity = TwinboxDiagnostics.StartActivity($"{message.Source} process", ActivityKind.Consumer, traceParent);
        activity?.SetTag("messaging.message.id", message.MessageId);
        activity?.SetTag("messaging.source.name", message.Source);
        return activity;
    }

    private async Task InvokeBatchAsync(BatchHandlerDescriptor handler, PreparedMessage[] items, CancellationToken cancellationToken)
    {
        if (inbox is not IBatchInboxStore batchInbox || !options.Value.Inbox.Enabled)
        {
            if (inbox is not null && options.Value.Inbox.Enabled)
            {
                // The store can only deduplicate one message at a time, so fall back to batches of one.
                foreach (var item in items)
                {
                    await InvokeAsync(handler, item.Message, item.Body, item.Context, cancellationToken).ConfigureAwait(false);
                }

                return;
            }

            var unguarded = scopeFactory.CreateAsyncScope();
            await using (unguarded.ConfigureAwait(false))
            {
                await RunBatchAsync(unguarded.ServiceProvider, handler, [.. items.Select(i => (i.Body, i.Context))], cancellationToken).ConfigureAwait(false);
                TwinboxDiagnostics.MessagesProcessed.Add(items.Length);
                return;
            }
        }

        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var now = time.GetUtcNow();
            var entries = items.Select(i => new InboxEntry(i.Message.MessageId, handler.ConsumerName, i.Message.Source, now)).ToArray();
            var processed = await batchInbox.TryProcessBatchAsync(
                entries,
                scope.ServiceProvider,
                (fresh, ct) => RunBatchAsync(scope.ServiceProvider, handler, [.. fresh.Select(i => (items[i].Body, items[i].Context))], ct),
                cancellationToken).ConfigureAwait(false);

            TwinboxDiagnostics.MessagesProcessed.Add(processed);
            TwinboxDiagnostics.DuplicatesSkipped.Add(items.Length - processed);
        }
    }

    private async Task InvokeAsync(
        HandlerDescriptor handler,
        IncomingMessage message,
        object body,
        MessageContext context,
        CancellationToken cancellationToken)
    {
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            if (inbox is null || !options.Value.Inbox.Enabled)
            {
                await RunAsync(scope.ServiceProvider, handler, body, context, cancellationToken).ConfigureAwait(false);
                TwinboxDiagnostics.MessagesProcessed.Add(1);
                return;
            }

            var entry = new InboxEntry(message.MessageId, handler.ConsumerName, message.Source, time.GetUtcNow());
            var processed = await inbox.TryProcessAsync(
                entry,
                scope.ServiceProvider,
                ct => RunAsync(scope.ServiceProvider, handler, body, context, ct),
                cancellationToken).ConfigureAwait(false);

            if (processed)
            {
                TwinboxDiagnostics.MessagesProcessed.Add(1);
            }
            else
            {
                LogDuplicate(message.MessageId, handler.ConsumerName);
                TwinboxDiagnostics.DuplicatesSkipped.Add(1);
            }
        }
    }

    private static async Task RunAsync(
        IServiceProvider services,
        HandlerDescriptor handler,
        object body,
        MessageContext context,
        CancellationToken cancellationToken)
    {
        if (handler is BatchHandlerDescriptor batchHandler)
        {
            await RunBatchAsync(services, batchHandler, [(body, context)], cancellationToken).ConfigureAwait(false);
            return;
        }

        using var _ = InboundContext.Enter(context);
        Func<Task> next = () => handler.InvokeAsync(services, body, context, cancellationToken);
        foreach (var filter in services.GetServices<IMessageFilter>().Reverse())
        {
            var inner = next;
            next = () => filter.InvokeAsync(body, context, inner, cancellationToken);
        }

        await next().ConfigureAwait(false);
    }

    private static async Task RunBatchAsync(
        IServiceProvider services,
        BatchHandlerDescriptor handler,
        IReadOnlyList<(object Message, MessageContext Context)> items,
        CancellationToken cancellationToken)
    {
        // Messages sent from a batch of one still inherit its correlation; a larger batch has no single context.
        using var _ = InboundContext.Enter(items.Count == 1 ? items[0].Context : null);
        Func<Task> next = () => handler.InvokeBatchAsync(services, items, cancellationToken);
        var filters = services.GetServices<IBatchMessageFilter>().Reverse().ToArray();
        if (filters.Length > 0)
        {
            IReadOnlyList<BatchItem<object>> batch = [.. items.Select(i => new BatchItem<object>(i.Message, i.Context))];
            foreach (var filter in filters)
            {
                var inner = next;
                next = () => filter.InvokeAsync(batch, inner, cancellationToken);
            }
        }

        await next().ConfigureAwait(false);
    }

    private object Deserialize(IncomingMessage message, Type messageType)
    {
        try
        {
            return serializer.Deserialize(message.Body.Span, messageType);
        }
        catch (Exception ex)
        {
            throw new PermanentDeliveryException($"Message {message.MessageId} could not be deserialized as {messageType}.", ex);
        }
    }

    private void HandleUnknown(IncomingMessage message, string reason)
    {
        if (options.Value.Inbox.UnknownMessages == UnknownMessagePolicy.Ignore)
        {
            LogUnknownIgnored(message.MessageId, message.MessageName, reason);
            return;
        }

        throw new PermanentDeliveryException($"Can't process '{message.MessageName}' (message {message.MessageId}): {reason}.");
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Skipping message {MessageId} for {Consumer}: already processed.")]
    private partial void LogDuplicate(string messageId, string consumer);

    [LoggerMessage(Level = LogLevel.Information, Message = "Ignoring message {MessageId} ('{MessageName}'): {Reason}.")]
    private partial void LogUnknownIgnored(string messageId, string messageName, string reason);

    private sealed record PreparedMessage(
        IncomingMessage Message,
        Type MessageType,
        object Body,
        MessageContext Context,
        string? Tenant,
        IReadOnlyList<HandlerDescriptor> Handlers);
}
