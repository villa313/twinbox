using System.Buffers;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Serialization;
using Twinbox.Storage;
using Twinbox.Tenancy;
using Twinbox.Transport;

namespace Twinbox.Webhooks;

/// <summary>Verifies, stores and acknowledges; handlers run later through the dispatcher. Bodies and secrets are never logged.</summary>
internal sealed partial class WebhookIngress(
    IOptions<TwinboxOptions> options,
    IDispatchSignal signal,
    IMessageSerializer serializer,
    TimeProvider time,
    ILogger<WebhookIngress> logger)
{
    private const int MaxEventIdLength = 256;
    private const int ReadChunkSize = 16 * 1024;

    private readonly ILogger _logger = logger;

    public async Task ReceiveAsync(HttpContext context, WebhookEndpoint endpoint)
    {
        var tenant = endpoint.TenantResolver?.Invoke(context);
        if (endpoint.TenantResolver is not null && string.IsNullOrEmpty(tenant))
        {
            LogRejected(endpoint.Provider, StatusCodes.Status404NotFound, "no tenant matches the request");
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        // Stores open tenant-aware scopes, so the webhook lands in this tenant's database.
        using var _ = TenantDirectory.Enter(tenant);
        var cancellationToken = context.RequestAborted;
        var body = await ReadBodyAsync(context, endpoint.MaxBodySize, cancellationToken).ConfigureAwait(false);
        if (body is null)
        {
            LogRejected(endpoint.Provider, StatusCodes.Status413PayloadTooLarge, "the body exceeds the size limit");
            context.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
            return;
        }

        var receivedAt = time.GetUtcNow();
        WebhookVerification verification;
        try
        {
            verification = endpoint.Verifier.Verify(new WebhookRequest
            {
                Headers = context.Request.Headers,
                Body = body,
                ReceivedAt = receivedAt,
                Services = context.RequestServices,
            });
        }
        catch (Exception ex)
        {
            // Usually a missing or invalid secret: fail closed, and let the provider retry once it's fixed.
            LogVerifierFailed(ex, endpoint.Provider);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
            return;
        }

        if (!verification.Succeeded || verification.EventId is not { Length: <= MaxEventIdLength } eventId)
        {
            var status = verification.Succeeded ? StatusCodes.Status400BadRequest : verification.StatusCode;
            LogRejected(endpoint.Provider, status, verification.Reason ?? "the event id is too long");
            context.Response.StatusCode = status;
            return;
        }

        var message = CreateMessage(context, endpoint, body, eventId, verification.EventType, receivedAt, tenant);
        try
        {
            await endpoint.Store.AppendAsync([message], cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            if (!await IsAlreadyStoredAsync(endpoint.Store, message.Id, ex, cancellationToken).ConfigureAwait(false))
            {
                // A 5xx makes the provider retry, so nothing is lost while the store is down.
                LogStoreFailed(ex, endpoint.Provider, eventId);
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                return;
            }

            LogDuplicate(endpoint.Provider, eventId);
            context.Response.StatusCode = StatusCodes.Status200OK;
            return;
        }

        signal.Notify();
        context.Response.StatusCode = StatusCodes.Status200OK;
    }

    /// <summary>Null when the body is larger than <paramref name="limit"/>; the limit is enforced while reading, not after.</summary>
    private static async Task<byte[]?> ReadBodyAsync(HttpContext context, long limit, CancellationToken cancellationToken)
    {
        var request = context.Request;
        if (request.ContentLength > limit)
        {
            return null;
        }

        if (context.Features.Get<IHttpMaxRequestBodySizeFeature>() is { IsReadOnly: false } sizeLimit)
        {
            sizeLimit.MaxRequestBodySize = limit;
        }

        using var buffer = new MemoryStream((int)Math.Min(request.ContentLength ?? ReadChunkSize, limit));
        var chunk = ArrayPool<byte>.Shared.Rent(ReadChunkSize);
        try
        {
            int read;
            while ((read = await request.Body.ReadAsync(chunk, cancellationToken).ConfigureAwait(false)) > 0)
            {
                if (buffer.Length + read > limit)
                {
                    return null;
                }

                buffer.Write(chunk, 0, read);
            }
        }
        catch (BadHttpRequestException ex) when (ex.StatusCode == StatusCodes.Status413PayloadTooLarge)
        {
            return null;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }

        return buffer.ToArray();
    }

    private static bool IsIntegrityViolation(Exception error)
    {
        for (var current = error; current is not null; current = current.InnerException)
        {
            if (current is DbException { SqlState: { } state } && state.StartsWith("23", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static Dictionary<string, string> ForwardedHeaders(IHeaderDictionary headers, IReadOnlyList<string> names)
    {
        var forwarded = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in names)
        {
            if (headers.TryGetValue(name, out var values) && values.Count > 0)
            {
                forwarded[name] = values.ToString();
            }
        }

        return forwarded;
    }

    private OutboxMessage CreateMessage(
        HttpContext context,
        WebhookEndpoint endpoint,
        byte[] body,
        string eventId,
        string? eventType,
        DateTimeOffset receivedAt,
        string? tenant)
    {
        var webhook = endpoint.Prototype with
        {
            Provider = endpoint.Provider,
            EventId = eventId,
            EventType = eventType,
            Body = Encoding.UTF8.GetString(body),
            ContentType = context.Request.ContentType,
            Headers = ForwardedHeaders(context.Request.Headers, endpoint.ForwardedHeaders),
            ReceivedAt = receivedAt,
        };

        return new OutboxMessage
        {
            Id = endpoint.MessageIdFor(eventId),
            MessageName = endpoint.MessageName,
            Transport = LocalTransport.TransportName,
            Destination = options.Value.ToPhysicalDestination(endpoint.Destination),
            TenantId = tenant,
            Payload = serializer.Serialize(webhook, endpoint.MessageType),
            ContentType = serializer.ContentType,
            TraceParent = Activity.Current is { IdFormat: ActivityIdFormat.W3C } activity ? activity.Id : null,
            CreatedAt = receivedAt,
            AvailableAt = receivedAt,
            Status = OutboxMessageStatus.Pending,
        };
    }

    private async Task<bool> IsAlreadyStoredAsync(IOutboxStore store, Guid id, Exception error, CancellationToken cancellationToken)
    {
        // Stores surface duplicate keys differently, so ask the store when it can say; otherwise trust only an
        // integrity-violation SQLSTATE (class 23), since the id is the only constraint an append can break.
        if (store is not IOutboxAdmin admin)
        {
            return IsIntegrityViolation(error);
        }

        try
        {
            return await admin.GetAsync(id, cancellationToken).ConfigureAwait(false) is not null;
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            LogDuplicateCheckFailed(ex);
            return false;
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejected a {Provider} webhook with {StatusCode}: {Reason}.")]
    private partial void LogRejected(string provider, int statusCode, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Couldn't verify a {Provider} webhook; answering 500. Check the endpoint's secrets.")]
    private partial void LogVerifierFailed(Exception error, string provider);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Webhook event {EventId} from {Provider} was already received; acknowledging the repeat.")]
    private partial void LogDuplicate(string provider, string eventId);

    [LoggerMessage(Level = LogLevel.Error, Message = "Couldn't store webhook event {EventId} from {Provider}; answering 500 so it is retried.")]
    private partial void LogStoreFailed(Exception error, string provider, string eventId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Couldn't check whether a failed webhook append was a duplicate.")]
    private partial void LogDuplicateCheckFailed(Exception error);
}
