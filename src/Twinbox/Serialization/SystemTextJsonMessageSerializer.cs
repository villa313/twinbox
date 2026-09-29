using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Twinbox.Serialization;

/// <summary>JSON serializer; pass options with a source-generated context for Native AOT.</summary>
public sealed class SystemTextJsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    public SystemTextJsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = new JsonSerializerOptions(options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web));
        _options.TypeInfoResolver ??= CreateReflectionResolver();
        _options.MakeReadOnly();
    }

    public string ContentType => "application/json";

    public byte[] Serialize<TMessage>(TMessage message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, (JsonTypeInfo<TMessage>)_options.GetTypeInfo(typeof(TMessage)));

    public object Deserialize(ReadOnlySpan<byte> body, Type messageType) =>
        JsonSerializer.Deserialize(body, _options.GetTypeInfo(messageType))
            ?? throw new JsonException($"Message body deserialized to null for {messageType}.");

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Only reached when reflection-based serialization is enabled for the app.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Only reached when reflection-based serialization is enabled for the app.")]
    private static DefaultJsonTypeInfoResolver CreateReflectionResolver() =>
        JsonSerializer.IsReflectionEnabledByDefault
            ? new DefaultJsonTypeInfoResolver()
            : throw new InvalidOperationException(
                "Reflection-based JSON serialization is disabled. Pass JsonSerializerOptions whose TypeInfoResolver is a source-generated JsonSerializerContext covering your message types.");
}
