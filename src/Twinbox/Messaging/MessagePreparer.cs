using System.Diagnostics;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Messaging;

internal sealed class MessagePreparer(
    MessageTypeRegistry registry,
    RouteTable routes,
    TransportRegistry transports,
    IMessageSerializer serializer,
    IMessageIdGenerator ids,
    TimeProvider time)
{
    public IReadOnlyList<OutboxMessage> Prepare<TMessage>(TMessage message, SendOptions? options, string? tenantId)
        where TMessage : class
    {
        // Route and serialize by the runtime type, so events collected as a base type still reach their own routes.
        var messageType = message.GetType();
        var messageRoutes = routes.Get(messageType);
        if (messageRoutes.Count == 0)
        {
            throw new InvalidOperationException(
                $"No route is configured for {messageType}. Add one with Route<{messageType.Name}>().To(\"destination\").");
        }

        var name = registry.GetOrAdd(messageType);
        var payload = serializer.Serialize(message, messageType);
        var now = time.GetUtcNow();
        var availableAt = options?.Delay is { } delay ? now + delay : now;
        var headers = options?.Headers ?? EmptyHeaders.Instance;
        var traceParent = Activity.Current is { IdFormat: ActivityIdFormat.W3C } activity ? activity.Id : null;

        var prepared = new OutboxMessage[messageRoutes.Count];
        for (var i = 0; i < prepared.Length; i++)
        {
            prepared[i] = new OutboxMessage
            {
                Id = ids.NewId(now),
                MessageName = name,
                Transport = messageRoutes[i].Transport ?? transports.ResolveDefaultName(),
                Destination = messageRoutes[i].Destination,
                PartitionKey = options?.PartitionKey,
                TenantId = tenantId,
                Payload = payload,
                ContentType = serializer.ContentType,
                Headers = headers,
                TraceParent = traceParent,
                CreatedAt = now,
                AvailableAt = availableAt,
                Status = OutboxMessageStatus.Pending,
            };
        }

        return prepared;
    }
}
