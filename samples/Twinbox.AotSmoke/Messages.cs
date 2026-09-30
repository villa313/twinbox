using System.Text.Json;
using System.Text.Json.Serialization;

namespace Twinbox.AotSmoke;

public sealed record OrderPlaced(int OrderId, string Sku, decimal Total);

public sealed record InvoiceRequested(int OrderId, string Email);

[MessageName("order-shipped")]
public sealed record OrderShipped(int OrderId, string TrackingNumber);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(InvoiceRequested))]
[JsonSerializable(typeof(OrderShipped))]
internal sealed partial class SmokeJsonContext : JsonSerializerContext;
