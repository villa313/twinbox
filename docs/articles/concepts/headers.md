# Headers and header profiles

## Headers Twinbox sends

Every outgoing message carries its id and name (as broker properties, headers or attributes, depending on the
transport) plus these headers when they apply. The names are constants on `Twinbox.Transport.TransportHeaders`.

| Header | Constant | Value |
|---|---|---|
| `twinbox-message-id` | `MessageId` | The message id (where the broker has no id property of its own). |
| `twinbox-message-name` | `MessageName` | The wire name (where the broker has no type or subject property). |
| `twinbox-partition-key` | `PartitionKey` | `SendOptions.PartitionKey`. |
| `twinbox-correlation-id` | `CorrelationId` | See [Correlation](correlation-and-reply.md). |
| `twinbox-reply-to` | `ReplyTo` | `SendOptions.ReplyTo`. |
| `twinbox-tenant` | `TenantId` | The tenant the message was sent for. See [Multi-tenancy](multi-tenancy.md). |
| `twinbox-delivery-attempt` | `DeliveryAttempt` | Which send attempt this is, starting at 1. |
| `traceparent` | `TraceParent` | W3C trace context of the producer span. |

On the receiving side, `MessageContext.Headers` holds everything the transport received.

## Adding your own

Per message:

```csharp
outbox.Send(evt, new SendOptions
{
    Headers = new Dictionary<string, string> { ["x-source"] = "checkout" },
});
```

For every message, with an outgoing filter:

```csharp
public class StampUser(IHttpContextAccessor http) : IOutgoingMessageFilter
{
    public void OnSending(object message, IDictionary<string, string> headers)
    {
        if (http.HttpContext?.User.Identity?.Name is { } user)
        {
            headers["x-user"] = user;
        }
    }
}

twinbox.AddOutgoingFilter<StampUser>();
```

Headers are stored with the outbox row, so they survive until the message is sent. See [Filters](filters.md).

## Header profiles

A header profile makes Twinbox also write, and read, another system's header names for the message id, name and
partition key. It is what lets a service on Twinbox and a service still on your current messaging library exchange
messages in both directions while you [migrate one service at a time](../migration.md).

```csharp
twinbox.UseHeaderProfile(HeaderProfile.Prefixed("legacy"));    // legacy-msg-id, legacy-msg-name
```

Or describe the names yourself:

```csharp
twinbox.UseHeaderProfile(new HeaderProfile(
    MessageId: "x-message-id",
    MessageName: "x-message-type",
    PartitionKey: "x-ordering-key")
{
    SentTime = "x-sent-at",                                  // ISO 8601 send time
    Constants = new Dictionary<string, string> { ["x-producer"] = "orders-service" },
});
```

How profiles behave:

- **Outgoing:** every registered profile's headers are added to every message, next to Twinbox's own.
- **Incoming:** if a profile's id header is present and non-empty, its id, name and partition key win over what the
  transport derived. That matters when a transport would otherwise fall back to a broker-assigned id that changes on
  redelivery.
- Call `UseHeaderProfile` more than once to speak several dialects; the first profile whose id header is present wins
  on the way in.

### CloudEvents

`HeaderProfile.CloudEvents(source)` writes [CloudEvents](https://cloudevents.io/) binary-mode attributes so non-.NET
consumers can read Twinbox messages: `ce-id`, `ce-type`, `ce-time`, `ce-specversion` (`1.0`) and `ce-source`.

```csharp
twinbox.UseHeaderProfile(HeaderProfile.CloudEvents("/orders-service"));             // HTTP style, "ce-"
twinbox.UseHeaderProfile(HeaderProfile.CloudEvents("/orders-service", prefix: "ce_")); // Kafka style
```

The prefix is `ce-` for HTTP, `ce_` for Kafka and `cloudEvents:` for AMQP. The body stays whatever the serializer
produces (JSON by default), with the content type set by the transport.
