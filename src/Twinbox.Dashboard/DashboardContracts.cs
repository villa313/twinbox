using System.Text.Json;
using System.Text.Json.Serialization;
using Twinbox.Storage;

namespace Twinbox.Dashboard;

internal sealed record ConfigResponse(
    string CsrfToken,
    bool ReadOnly,
    bool ShowPayloads,
    IReadOnlyList<StoreInfo> Stores,
    IReadOnlyList<string> Tenants);

internal sealed record StoreInfo(int Id, string Name, bool Browsable);

internal sealed record StatsResponse(IReadOnlyList<StoreStats> Stores, DateTimeOffset GeneratedAt);

internal sealed record StoreStats(
    int Store,
    string StoreName,
    string? Tenant,
    long? Pending,
    long? Dead,
    double? OldestPendingAgeSeconds,
    string? Error);

internal sealed record MessagesResponse(IReadOnlyList<MessageSummary> Messages, string? NextCursor);

internal sealed record MessageSummary(
    Guid Id,
    string MessageName,
    string Transport,
    string Destination,
    string? PartitionKey,
    string? TenantId,
    string Status,
    int Attempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset AvailableAt,
    DateTimeOffset? SentAt,
    string? LastError,
    int PayloadBytes)
{
    public static MessageSummary From(OutboxMessage m) => new(
        m.Id,
        m.MessageName,
        m.Transport,
        m.Destination,
        m.PartitionKey,
        m.TenantId,
        m.Status.ToString(),
        m.Attempts,
        m.CreatedAt,
        m.AvailableAt,
        m.SentAt,
        m.LastError,
        m.Payload.Length);
}

internal sealed record MessageDetail(
    MessageSummary Message,
    string ContentType,
    string? TraceParent,
    string? LeaseOwner,
    DateTimeOffset? LeaseUntil,
    IReadOnlyDictionary<string, string> Headers,
    bool PayloadShown,
    string? Payload,
    bool PayloadTruncated);

internal sealed record MutationRequest(int? Store, string? Tenant, IReadOnlyList<Guid>? Ids);

internal sealed record ReplayAllRequest(int? Store, string? Tenant, string? Destination, string? Name, string? Search);

internal sealed record MutationResponse(int Changed);

internal sealed record ErrorResponse(string Error);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(ConfigResponse))]
[JsonSerializable(typeof(StatsResponse))]
[JsonSerializable(typeof(MessagesResponse))]
[JsonSerializable(typeof(MessageDetail))]
[JsonSerializable(typeof(MutationRequest))]
[JsonSerializable(typeof(ReplayAllRequest))]
[JsonSerializable(typeof(MutationResponse))]
[JsonSerializable(typeof(ErrorResponse))]
internal sealed partial class DashboardJsonContext : JsonSerializerContext;
