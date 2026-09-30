# Serialization

Payloads are JSON by default, written with `System.Text.Json` and `JsonSerializerDefaults.Web` (camelCase property
names, case-insensitive reading). The content type is `application/json`.

## Custom JSON options

```csharp
var json = new JsonSerializerOptions(JsonSerializerDefaults.Web)
{
    Converters = { new JsonStringEnumConverter() },
};

twinbox.UseSerializer(new SystemTextJsonMessageSerializer(json));
```

The options are copied and frozen when the serializer is created.

## Native AOT and trimming

With reflection-based serialization disabled, give Twinbox a source-generated context covering your message types.
`UseJsonTypeInfoResolver` keeps the default camelCase format:

```csharp
[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(OrderShipped))]
internal partial class MessagesJsonContext : JsonSerializerContext;

twinbox.UseJsonTypeInfoResolver(MessagesJsonContext.Default);
```

A message type the resolver doesn't cover fails with an error instead of falling back to reflection.
`samples/Twinbox.AotSmoke` is published with Native AOT in CI. Twinbox.EntityFrameworkCore, SqlServer, Oracle,
MongoDB, Kafka, Pulsar and GooglePubSub aren't AOT compatible because of their client libraries.

## Your own serializer

Implement `IMessageSerializer`:

```csharp
public interface IMessageSerializer
{
    string ContentType { get; }
    byte[] Serialize(object message, Type messageType);
    object Deserialize(ReadOnlySpan<byte> body, Type messageType);
}
```

`messageType` is the runtime type being sent, or the type registered for the incoming message name. A
`Deserialize` that throws dead-letters the message: bad bytes won't improve on retry.

The serializer is global. The content type is stored with each outbox row and sent with the message, but incoming
messages are always deserialized with the registered serializer, whatever content type they arrive with.

## Versioning messages

- Pin wire names with `[MessageName("...")]` so class renames don't break receivers.
- Additive changes (new optional properties) are safe with JSON in both directions.
- For breaking changes, introduce a new type with a new name (`orders.placed.v2`) and route both while receivers
  catch up.
