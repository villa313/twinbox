using System.Diagnostics;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Messaging;

internal sealed class MessagePreparer(
    Microsoft.Extensions.Options.IOptions<TwinboxOptions> options,
    MessageTypeRegistry registry,
    RouteTable routes,
    TransportRegistry transports,
    IMessageSerializer serializer,
    IMessageIdGenerator ids,
    TimeProvider time)
{
    /// <summary>A <paramref name="physicalDestination"/> is an address as the broker knows it (a reply address) and isn't prefixed.</summary>
    public IReadOnlyList<OutboxMessage> Prepare<TMessage>(TMessage message, SendOptions? sendOptions, string? tenantId, bool physicalDestination = false)
        where TMessage : class
    {
        // Route and serialize by the runtime type, so events collected as a base type still reach their own routes.
        var messageType = message.GetType();
        var messageRoutes = sendOptions?.Destination is { } destination
            ? [new Route(physicalDestination ? destination : options.Value.ToPhysicalDestination(destination), sendOptions.Transport)]
            : Prefixed(routes.Get(messageType));
        if (messageRoutes.Count == 0)
        {
            throw new InvalidOperationException(
                $"No route is configured for {messageType}. Add one with Route<{messageType.Name}>().To(\"destination\").");
        }

        var name = registry.GetOrAdd(messageType);
        var payload = serializer.Serialize(message, messageType);
        var now = time.GetUtcNow();
        var availableAt = sendOptions?.Delay is { } delay ? now + delay : now;
        var headers = sendOptions?.Headers ?? EmptyHeaders.Instance;
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
                // Empty means "none": some databases (Oracle) can't tell an empty string from NULL.
                PartitionKey = NullIfEmpty(sendOptions?.PartitionKey),
                TenantId = NullIfEmpty(tenantId),
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

    /// <summary>Builds outbox rows for a body produced elsewhere, one per route, with ids derived from its old id.</summary>
    public IReadOnlyList<OutboxMessage> PrepareImported(
        Type messageType,
        string messageName,
        string legacyId,
        byte[] payload,
        string? tenantId,
        IReadOnlyDictionary<string, string> headers,
        string? partitionKey)
    {
        var now = time.GetUtcNow();
        return [.. Prefixed(routes.Get(messageType)).Select(route => new OutboxMessage
        {
            Id = Migration.ImportedMessageIds.For(legacyId, route.Destination),
            MessageName = messageName,
            Transport = route.Transport ?? transports.ResolveDefaultName(),
            Destination = route.Destination,
            PartitionKey = NullIfEmpty(partitionKey),
            TenantId = NullIfEmpty(tenantId),
            Headers = headers,
            Payload = payload,
            ContentType = serializer.ContentType,
            CreatedAt = now,
            AvailableAt = now,
            Status = OutboxMessageStatus.Pending,
        })];
    }

    private IReadOnlyList<Route> Prefixed(IReadOnlyList<Route> routes) =>
        [.. routes.Select(r => r with { Destination = options.Value.ToPhysicalDestination(r.Destination) })];

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
