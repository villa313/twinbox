using System.Diagnostics;
using Twinbox.Diagnostics;
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
        var headers = WithTraceContext(sendOptions?.Headers ?? EmptyHeaders.Instance, Activity.Current);

        var prepared = new OutboxMessage[messageRoutes.Count];
        for (var i = 0; i < prepared.Length; i++)
        {
            var id = ids.NewId(now);
            var transport = messageRoutes[i].Transport ?? transports.ResolveDefaultName();
            using var create = StartCreateActivity(id, transport, messageRoutes[i].Destination);
            prepared[i] = new OutboxMessage
            {
                Id = id,
                MessageName = name,
                Transport = transport,
                Destination = messageRoutes[i].Destination,
                // Empty means "none": some databases (Oracle) can't tell an empty string from NULL.
                PartitionKey = NullIfEmpty(sendOptions?.PartitionKey),
                TenantId = NullIfEmpty(tenantId),
                Payload = payload,
                ContentType = serializer.ContentType,
                Headers = headers,
                TraceParent = (create ?? Activity.Current) is { IdFormat: ActivityIdFormat.W3C } parent ? parent.Id : null,
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

    /// <summary>The send span is a child of this one, so the request's trace shows the message being queued.</summary>
    private static Activity? StartCreateActivity(Guid id, string transport, string destination)
    {
        var activity = TwinboxDiagnostics.ActivitySource.StartActivity($"create {destination}", ActivityKind.Producer);
        activity?.SetTag("messaging.system", transport);
        activity?.SetTag("messaging.operation.type", "create");
        activity?.SetTag("messaging.operation.name", "create");
        activity?.SetTag("messaging.destination.name", destination);
        activity?.SetTag("messaging.message.id", id.ToString());
        return activity;
    }

    /// <summary>Only traceparent has a column; tracestate and baggage ride in the headers, which every transport carries.</summary>
    private static IReadOnlyDictionary<string, string> WithTraceContext(IReadOnlyDictionary<string, string> headers, Activity? current)
    {
        var traceState = current?.TraceStateString;
        var baggage = current is null ? null : W3CBaggage.Encode(current.Baggage);
        if (string.IsNullOrEmpty(traceState) && baggage is null)
        {
            return headers;
        }

        var withContext = new Dictionary<string, string>(headers);
        if (!string.IsNullOrEmpty(traceState))
        {
            withContext[TransportHeaders.TraceState] = traceState;
        }

        if (baggage is not null)
        {
            withContext[TransportHeaders.Baggage] = baggage;
        }

        return withContext;
    }

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
