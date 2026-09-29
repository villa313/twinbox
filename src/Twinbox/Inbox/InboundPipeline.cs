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
        message.Headers.TryGetValue(TransportHeaders.TraceParent, out var traceParent);
        using var activity = TwinboxDiagnostics.StartActivity($"{message.Source} process", ActivityKind.Consumer, traceParent);
        activity?.SetTag("messaging.message.id", message.MessageId);
        activity?.SetTag("messaging.source.name", message.Source);

        if (!registry.TryResolve(message.MessageName, out var messageType))
        {
            HandleUnknown(message, "no message type is registered");
            return;
        }

        var messageHandlers = handlers.For(messageType);
        if (messageHandlers.Count == 0)
        {
            // Acknowledging a message nobody handled would look like a successful delivery.
            HandleUnknown(message, "no handler is registered");
            return;
        }

        message.Headers.TryGetValue(TransportHeaders.TenantId, out var tenant);
        using var _ = TenantScope.Enter(tenant);
        var body = Deserialize(message, messageType);
        var context = new MessageContext(
            message.MessageId, message.MessageName, message.Source, message.Headers, message.DeliveryAttempt, message.PartitionKey);

        foreach (var handler in messageHandlers)
        {
            await InvokeAsync(handler, message, body, context, cancellationToken).ConfigureAwait(false);
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
                await handler.InvokeAsync(scope.ServiceProvider, body, context, cancellationToken).ConfigureAwait(false);
                TwinboxDiagnostics.MessagesProcessed.Add(1);
                return;
            }

            var entry = new InboxEntry(message.MessageId, handler.ConsumerName, message.Source, time.GetUtcNow());
            var processed = await inbox.TryProcessAsync(
                entry,
                scope.ServiceProvider,
                ct => handler.InvokeAsync(scope.ServiceProvider, body, context, ct),
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
}
