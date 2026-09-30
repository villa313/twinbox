using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Twinbox.Storage;

namespace Twinbox.Dashboard;

internal sealed partial class DashboardEndpoints
{
    public const int MaxPayloadBytes = 64 * 1024;

    private const int MaxTake = 200;
    private const int MaxIdsPerRequest = 1000;
    private const int ReplayAllPageSize = 500;
    private const int MaxFilterLength = 256;

    private static readonly DashboardJsonContext Json = DashboardJsonContext.Default;

    private readonly ILogger _logger;
    private readonly TwinboxDashboardOptions _options;
    private readonly DashboardStores _stores;
    private readonly DashboardCsrf _csrf;
    private readonly TimeProvider _time;
    private readonly string _prefix;
    private readonly Lazy<string> _page = new(LoadPage);

    public DashboardEndpoints(
        ILogger logger,
        TwinboxDashboardOptions options,
        DashboardStores stores,
        DashboardCsrf csrf,
        TimeProvider time,
        string prefix)
    {
        _logger = logger;
        _options = options;
        _stores = stores;
        _csrf = csrf;
        _time = time;
        _prefix = prefix;
    }

    public void WarnIfAnonymous()
    {
        if (_options.AllowAnonymous)
        {
            LogAnonymousAllowed(_prefix);
        }
    }

    /// <summary>Wraps a handler with the checks every dashboard request needs before it touches a store.</summary>
    public RequestDelegate Guard(Func<HttpContext, Task> handler, bool mutates = false) => async context =>
    {
        if (!_options.AllowAnonymous && !HasAuthorizationPolicy(context))
        {
            await ErrorAsync(
                context,
                StatusCodes.Status403Forbidden,
                "The Twinbox dashboard needs an authorization policy: call RequireAuthorization on MapTwinboxDashboard.").ConfigureAwait(false);
            return;
        }

        if (mutates && await RefuseMutationAsync(context).ConfigureAwait(false))
        {
            return;
        }

        context.Response.Headers.CacheControl = "no-store";
        context.Response.Headers.XContentTypeOptions = "nosniff";
        await handler(context).ConfigureAwait(false);
    };

    public async Task PageAsync(HttpContext context)
    {
        var nonce = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));
        var basePath = WebUtility.HtmlEncode(context.Request.PathBase.Add(new PathString(_prefix)).Value ?? string.Empty);
        var html = _page.Value.Replace("{{NONCE}}", nonce, StringComparison.Ordinal).Replace("{{BASE}}", basePath, StringComparison.Ordinal);

        var headers = context.Response.Headers;
        headers.ContentSecurityPolicy =
            $"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; connect-src 'self'; img-src 'self' data:; " +
            "base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
        headers["Referrer-Policy"] = "no-referrer";
        headers.XFrameOptions = "DENY";
        context.Response.ContentType = "text/html; charset=utf-8";
        await context.Response.WriteAsync(html, context.RequestAborted).ConfigureAwait(false);
    }

    public async Task ConfigAsync(HttpContext context)
    {
        var tenants = await _stores.TenantsAsync(context.RequestAborted).ConfigureAwait(false);
        await WriteAsync(
            context,
            new ConfigResponse(
                _csrf.Issue(context),
                _options.ReadOnly,
                _options.ShowPayloads,
                [.. _stores.All.Select(s => new StoreInfo(s.Id, s.Name, s.Admin is not null))],
                [.. tenants.OfType<string>()]),
            Json.ConfigResponse).ConfigureAwait(false);
    }

    public async Task StatsAsync(HttpContext context)
    {
        var ct = context.RequestAborted;
        var now = _time.GetUtcNow();
        var rows = new List<StoreStats>();
        foreach (var tenant in await _stores.TenantsAsync(ct).ConfigureAwait(false))
        {
            foreach (var store in _stores.All)
            {
                rows.Add(await StatsForAsync(store, tenant, now, ct).ConfigureAwait(false));
            }
        }

        await WriteAsync(context, new StatsResponse(rows, now), Json.StatsResponse).ConfigureAwait(false);
    }

    public async Task MessagesAsync(HttpContext context)
    {
        var request = context.Request.Query;
        if (!TryParseQuery(request, out var query, out var problem))
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, problem).ConfigureAwait(false);
            return;
        }

        var target = await TargetAsync(context, Int(request["store"]), request["tenant"]).ConfigureAwait(false);
        if (target is null)
        {
            return;
        }

        var page = await DashboardStores.InTenantAsync(
            target.Value.Tenant,
            () => target.Value.Admin.QueryAsync(query, context.RequestAborted)).ConfigureAwait(false);
        await WriteAsync(
            context,
            new MessagesResponse([.. page.Messages.Select(MessageSummary.From)], page.NextCursor),
            Json.MessagesResponse).ConfigureAwait(false);
    }

    public async Task MessageAsync(HttpContext context)
    {
        if (!Guid.TryParse(context.Request.RouteValues["id"]?.ToString(), out var id))
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, "The message id is not a GUID.").ConfigureAwait(false);
            return;
        }

        var request = context.Request.Query;
        var target = await TargetAsync(context, Int(request["store"]), request["tenant"]).ConfigureAwait(false);
        if (target is null)
        {
            return;
        }

        var message = await DashboardStores.InTenantAsync(
            target.Value.Tenant,
            () => target.Value.Admin.GetAsync(id, context.RequestAborted)).ConfigureAwait(false);
        if (message is null)
        {
            await ErrorAsync(context, StatusCodes.Status404NotFound, "No outbox message has that id.").ConfigureAwait(false);
            return;
        }

        await WriteAsync(context, Detail(message), Json.MessageDetail).ConfigureAwait(false);
    }

    public Task ReplayAsync(HttpContext context) => MutateAsync(context, replay: true);

    public Task DeleteAsync(HttpContext context) => MutateAsync(context, replay: false);

    public async Task ReplayAllDeadAsync(HttpContext context)
    {
        var body = await ReadAsync(context, Json.ReplayAllRequest).ConfigureAwait(false);
        if (body is null)
        {
            return;
        }

        var query = new OutboxQuery
        {
            Status = OutboxMessageStatus.Dead,
            Destination = Filter(body.Destination),
            MessageName = Filter(body.Name),
            Search = Filter(body.Search),
            Take = ReplayAllPageSize,
        };
        if (Longest(query) > MaxFilterLength)
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, $"Filters are limited to {MaxFilterLength} characters.").ConfigureAwait(false);
            return;
        }

        var target = await TargetAsync(context, body.Store, body.Tenant).ConfigureAwait(false);
        if (target is null)
        {
            return;
        }

        var (admin, tenant, store) = target.Value;
        var changed = await DashboardStores.InTenantAsync(tenant, async () =>
        {
            var total = 0;
            for (var next = query; ; )
            {
                // Replayed messages leave the Dead filter, but the cursor still moves strictly backwards.
                var page = await admin.QueryAsync(next, context.RequestAborted).ConfigureAwait(false);
                if (page.Messages.Count > 0)
                {
                    total += await admin.ReplayAsync([.. page.Messages.Select(m => m.Id)], _time.GetUtcNow(), context.RequestAborted)
                        .ConfigureAwait(false);
                }

                if (page.NextCursor is null)
                {
                    return total;
                }

                next = next with { Cursor = page.NextCursor };
            }
        }).ConfigureAwait(false);

        LogMutation(UserOf(context), "replayed all dead", changed, store, tenant);
        await WriteAsync(context, new MutationResponse(changed), Json.MutationResponse).ConfigureAwait(false);
    }

    private static bool HasAuthorizationPolicy(HttpContext context)
    {
        var metadata = context.GetEndpoint()?.Metadata;
        if (metadata is null || metadata.GetMetadata<IAllowAnonymous>() is not null)
        {
            return false;
        }

        return metadata.GetMetadata<IAuthorizeData>() is not null || metadata.GetMetadata<AuthorizationPolicy>() is not null;
    }

    private static bool TryParseQuery(IQueryCollection request, out OutboxQuery query, out string problem)
    {
        query = new OutboxQuery();
        problem = string.Empty;

        OutboxMessageStatus? status = null;
        if (Filter(request["status"]) is { } text)
        {
            if (!Enum.TryParse<OutboxMessageStatus>(text, ignoreCase: true, out var parsed) || !Enum.IsDefined(parsed) || int.TryParse(text, out _))
            {
                problem = "Status must be Pending, Processing, Sent or Dead.";
                return false;
            }

            status = parsed;
        }

        var take = Int(request["take"]) ?? 50;
        if (take is < 1 or > MaxTake)
        {
            problem = $"Take must be between 1 and {MaxTake}.";
            return false;
        }

        query = new OutboxQuery
        {
            Status = status,
            Destination = Filter(request["destination"]),
            MessageName = Filter(request["name"]),
            Search = Filter(request["search"]),
            Cursor = Filter(request["cursor"]),
            Take = take,
        };
        if (Longest(query) > MaxFilterLength)
        {
            problem = $"Filters are limited to {MaxFilterLength} characters.";
            return false;
        }

        return true;
    }

    private static int Longest(OutboxQuery query) =>
        new[] { query.Destination, query.MessageName, query.Search, query.Cursor }.Max(v => v?.Length ?? 0);

    private static string? Filter(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static int? Int(string? value) =>
        int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) ? parsed : null;

    private static string UserOf(HttpContext context) => context.User.Identity?.Name ?? "anonymous";

    private static Task WriteAsync<T>(HttpContext context, T value, JsonTypeInfo<T> typeInfo) =>
        context.Response.WriteAsJsonAsync(value, typeInfo, contentType: null, context.RequestAborted);

    private static Task ErrorAsync(HttpContext context, int status, string error)
    {
        context.Response.StatusCode = status;
        context.Response.Headers.CacheControl = "no-store";
        return WriteAsync(context, new ErrorResponse(error), Json.ErrorResponse);
    }

    private static async Task<T?> ReadAsync<T>(HttpContext context, JsonTypeInfo<T> typeInfo)
        where T : class
    {
        try
        {
            var body = await context.Request.ReadFromJsonAsync(typeInfo, context.RequestAborted).ConfigureAwait(false);
            if (body is null)
            {
                await ErrorAsync(context, StatusCodes.Status400BadRequest, "The request body is empty.").ConfigureAwait(false);
            }

            return body;
        }
        catch (JsonException)
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, "The request body is not valid JSON.").ConfigureAwait(false);
            return null;
        }
    }

    private static string LoadPage()
    {
        using var stream = typeof(DashboardEndpoints).Assembly.GetManifestResourceStream("Twinbox.Dashboard.index.html")
            ?? throw new InvalidOperationException("The dashboard page is missing from the Twinbox.Dashboard assembly.");
        using var reader = new StreamReader(stream, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private MessageDetail Detail(OutboxMessage message)
    {
        var shown = _options.ShowPayloads;
        var truncated = shown && message.Payload.Length > MaxPayloadBytes;
        var payload = shown ? Encoding.UTF8.GetString(message.Payload, 0, Math.Min(message.Payload.Length, MaxPayloadBytes)) : null;
        return new MessageDetail(
            MessageSummary.From(message),
            message.ContentType,
            message.TraceParent,
            message.LeaseOwner,
            message.LeaseUntil,
            message.Headers,
            shown,
            payload,
            truncated);
    }

    private async Task<bool> RefuseMutationAsync(HttpContext context)
    {
        if (_options.ReadOnly)
        {
            await ErrorAsync(context, StatusCodes.Status403Forbidden, "The dashboard is read-only.").ConfigureAwait(false);
            return true;
        }

        if (DashboardCsrf.IsCrossSite(context.Request) || !_csrf.IsValid(context))
        {
            await ErrorAsync(context, StatusCodes.Status403Forbidden, $"Missing or invalid {DashboardCsrf.HeaderName} header.").ConfigureAwait(false);
            return true;
        }

        if (!context.Request.HasJsonContentType())
        {
            await ErrorAsync(context, StatusCodes.Status415UnsupportedMediaType, "Send the request body as application/json.").ConfigureAwait(false);
            return true;
        }

        return false;
    }

    private async Task MutateAsync(HttpContext context, bool replay)
    {
        var body = await ReadAsync(context, Json.MutationRequest).ConfigureAwait(false);
        if (body is null)
        {
            return;
        }

        if (body.Ids is not { Count: > 0 and <= MaxIdsPerRequest } ids)
        {
            await ErrorAsync(context, StatusCodes.Status400BadRequest, $"Send between 1 and {MaxIdsPerRequest} message ids.").ConfigureAwait(false);
            return;
        }

        var target = await TargetAsync(context, body.Store, body.Tenant).ConfigureAwait(false);
        if (target is null)
        {
            return;
        }

        var (admin, tenant, store) = target.Value;
        var changed = await DashboardStores.InTenantAsync(
            tenant,
            () => replay
                ? admin.ReplayAsync([.. ids], _time.GetUtcNow(), context.RequestAborted)
                : admin.DeleteAsync([.. ids], context.RequestAborted)).ConfigureAwait(false);

        LogMutation(UserOf(context), replay ? "replayed" : "deleted", changed, store, tenant);
        await WriteAsync(context, new MutationResponse(changed), Json.MutationResponse).ConfigureAwait(false);
    }

    /// <summary>Resolves the store and tenant a request targets, writing the error response when either is unknown.</summary>
    private async Task<(IOutboxAdmin Admin, string? Tenant, string Store)?> TargetAsync(HttpContext context, int? storeId, string? requestedTenant)
    {
        var store = _stores.Find(storeId);
        if (store?.Admin is not { } admin)
        {
            var error = store is null ? "Unknown store." : $"The {store.Name} store does not support browsing messages.";
            await ErrorAsync(context, StatusCodes.Status404NotFound, error).ConfigureAwait(false);
            return null;
        }

        var (known, tenant) = await _stores.ResolveTenantAsync(requestedTenant, context.RequestAborted).ConfigureAwait(false);
        if (!known)
        {
            await ErrorAsync(context, StatusCodes.Status404NotFound, "Unknown tenant.").ConfigureAwait(false);
            return null;
        }

        return (admin, tenant, store.Name);
    }

    private async Task<StoreStats> StatsForAsync(StoreEntry store, string? tenant, DateTimeOffset now, CancellationToken cancellationToken)
    {
        try
        {
            var stats = await DashboardStores.InTenantAsync(tenant, () => store.Store.GetStatisticsAsync(cancellationToken)).ConfigureAwait(false);
            var age = stats.OldestPendingAvailableAt is { } oldest ? Math.Max(0, (now - oldest).TotalSeconds) : (double?)null;
            return new StoreStats(store.Id, store.Name, tenant, stats.PendingCount, stats.DeadCount, age, null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // One unreachable tenant database shouldn't hide the others; the failure is logged and shown on its card.
            LogStatisticsFailed(ex, store.Name, tenant);
            return new StoreStats(store.Id, store.Name, tenant, null, null, null, "Statistics are unavailable; see the application log.");
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "The Twinbox dashboard at {Prefix} allows anonymous access. Attach an authorization policy outside local development.")]
    private partial void LogAnonymousAllowed(string prefix);

    [LoggerMessage(Level = LogLevel.Information, Message = "{User} {Action} {Count} outbox messages in the {Store} store (tenant {Tenant}) from the Twinbox dashboard")]
    private partial void LogMutation(string user, string action, int count, string store, string? tenant);

    [LoggerMessage(Level = LogLevel.Warning, Message = "The Twinbox dashboard could not read statistics from the {Store} store (tenant {Tenant})")]
    private partial void LogStatisticsFailed(Exception exception, string store, string? tenant);
}
