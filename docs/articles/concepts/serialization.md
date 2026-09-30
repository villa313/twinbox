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

With reflection-based serialization disabled, pass options whose `TypeInfoResolver` is a source-generated context
covering your message types:

```csharp
[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(OrderShipped))]
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
internal partial class MessagesJsonContext : JsonSerializerContext;

twinbox.UseSerializer(new SystemTextJsonMessageSerializer(
    new JsonSerializerOptions(JsonSerializerDefaults.Web) { TypeInfoResolver = MessagesJsonContext.Default }));
```

Without a resolver, the serializer throws at construction time when reflection is disabled, rather than failing later
on the first message.

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
