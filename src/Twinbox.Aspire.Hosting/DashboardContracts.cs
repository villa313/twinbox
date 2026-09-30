using System.Text.Json;
using System.Text.Json.Serialization;

namespace Twinbox.Aspire.Hosting;

internal sealed record DashboardConfig(string? CsrfToken, bool ReadOnly, IReadOnlyList<DashboardStore>? Stores, IReadOnlyList<string>? Tenants);

internal sealed record DashboardStore(int Id, string? Name, bool Browsable);

internal sealed record ReplayAllRequest(int Store, string? Tenant);

internal sealed record MutationResponse(int Changed);

internal sealed record ErrorResponse(string? Error);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(DashboardConfig))]
[JsonSerializable(typeof(ReplayAllRequest))]
[JsonSerializable(typeof(MutationResponse))]
[JsonSerializable(typeof(ErrorResponse))]
internal sealed partial class DashboardJsonContext : JsonSerializerContext;
