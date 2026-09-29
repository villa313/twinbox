using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Diagnostics;
using Twinbox.Storage;
using Twinbox.Tenancy;
using Twinbox.Transport;

namespace Twinbox.Dispatch;

internal sealed partial class OutboxDispatcher(
    IEnumerable<IOutboxStore> stores,
    TenantDirectory tenants,
    TransportRegistry transports,
    IEnumerable<IDeadLetterObserver> deadLetterObservers,
    IOptions<TwinboxOptions> options,
    TimeProvider time,
    ILogger<OutboxDispatcher> logger) : IOutboxDispatcher
{
    private readonly ILogger _logger = logger;

    private const int MaxErrorLength = 2000;

    private readonly ConcurrentDictionary<(string Transport, string Destination), CircuitBreaker> _breakers = new();
    private readonly IDeadLetterObserver[] _observers = [.. deadLetterObservers];
    private readonly IOutboxStore[] _stores = [.. stores];

    public async Task<int> DispatchBatchAsync(CancellationToken cancellationToken)
    {
        var claimed = 0;
        foreach (var tenant in await tenants.GetTenantsAsync(cancellationToken).ConfigureAwait(false))
        {
            using var _ = TenantScope.Enter(tenant);
            foreach (var store in _stores)
            {
                claimed += await DispatchBatchAsync(store, cancellationToken).ConfigureAwait(false);
            }
        }

        return claimed;
    }

    private async Task<int> DispatchBatchAsync(IOutboxStore store, CancellationToken cancellationToken)
    {
        var settings = options.Value;
        var owner = settings.InstanceId;
        var claimed = await store.ClaimAsync(
            new OutboxClaim(owner, settings.Dispatcher.BatchSize, time.GetUtcNow(), settings.Dispatcher.LeaseDuration),
            cancellationToken).ConfigureAwait(false);

        if (claimed.Count == 0)
        {
            return 0;
        }

        var outcomes = new ConcurrentBag<DispatchOutcome>();
        var groups = claimed.GroupBy(m => (m.Transport, m.Destination)).ToArray();
        await Parallel.ForEachAsync(
            groups,
            new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, settings.Dispatcher.MaxDegreeOfParallelism), CancellationToken = cancellationToken },
            async (group, ct) => await DispatchGroupAsync(group.Key, [.. group], settings, outcomes, ct).ConfigureAwait(false))
            .ConfigureAwait(false);

        await store.CompleteAsync(owner, [.. outcomes], cancellationToken).ConfigureAwait(false);
        return claimed.Count;
    }

    private async Task DispatchGroupAsync(
        (string Transport, string Destination) key,
        OutboxMessage[] messages,
        TwinboxOptions settings,
        ConcurrentBag<DispatchOutcome> outcomes,
        CancellationToken cancellationToken)
    {
        var destination = settings.Destinations.GetValueOrDefault(key.Destination);
        var retry = destination?.Retry ?? settings.Retry;
        var breaker = _breakers.GetOrAdd(key, _ => new CircuitBreaker(destination?.CircuitBreaker ?? settings.CircuitBreaker));

        if (!transports.TryGet(key.Transport, out var transport))
        {
            var error = new PermanentDeliveryException($"No transport named '{key.Transport}' is registered.");
            foreach (var message in messages)
            {
                outcomes.Add(await DeadLetterAsync(message, message.Attempts + 1, error, cancellationToken).ConfigureAwait(false));
            }

            return;
        }

        foreach (var message in messages)
        {
            var now = time.GetUtcNow();
            if (!breaker.TryAllow(now, out var retryAt))
            {
                outcomes.Add(new DispatchOutcome(message.Id, OutboxMessageStatus.Pending, message.Attempts, AvailableAt: retryAt));
                continue;
            }

            try
            {
                await SendAsync(transport, message, cancellationToken).ConfigureAwait(false);
                breaker.RecordSuccess();
                var sentAt = time.GetUtcNow();
                outcomes.Add(new DispatchOutcome(message.Id, OutboxMessageStatus.Sent, message.Attempts + 1, SentAt: sentAt));
                RecordDelivered(message, sentAt);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                outcomes.Add(await HandleFailureAsync(message, ex, retry, breaker, cancellationToken).ConfigureAwait(false));
            }
        }
    }

    private static async Task SendAsync(ITransport transport, OutboxMessage message, CancellationToken cancellationToken)
    {
        using var activity = TwinboxDiagnostics.StartActivity($"{message.Destination} send", ActivityKind.Producer, message.TraceParent);
        activity?.SetTag("messaging.system", transport.Name);
        activity?.SetTag("messaging.destination.name", message.Destination);
        activity?.SetTag("messaging.message.id", message.Id);

        var headers = new Dictionary<string, string>(message.Headers);
        var traceParent = activity?.Id ?? message.TraceParent;
        if (traceParent is not null)
        {
            headers[TransportHeaders.TraceParent] = traceParent;
        }

        if (message.TenantId is not null)
        {
            headers[TransportHeaders.TenantId] = message.TenantId;
        }

        try
        {
            await transport.SendAsync(
                new TransportMessage(
                    message.Id.ToString(),
                    message.MessageName,
                    message.Destination,
                    message.Payload,
                    message.ContentType,
                    headers,
                    message.PartitionKey),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    private async Task<DispatchOutcome> HandleFailureAsync(
        OutboxMessage message,
        Exception error,
        RetryOptions retry,
        CircuitBreaker breaker,
        CancellationToken cancellationToken)
    {
        var attempts = message.Attempts + 1;
        var now = time.GetUtcNow();
        TwinboxDiagnostics.SendFailures.Add(1, new KeyValuePair<string, object?>("destination", message.Destination));

        if (error is PermanentDeliveryException || attempts >= retry.MaxAttempts)
        {
            return await DeadLetterAsync(message, attempts, error, cancellationToken).ConfigureAwait(false);
        }

        breaker.RecordFailure(now);
        var availableAt = now + RetrySchedule.GetDelay(retry, attempts, Random.Shared);
        LogSendFailed(error, message.Id, message.Destination, attempts, availableAt);
        return new DispatchOutcome(message.Id, OutboxMessageStatus.Pending, attempts, AvailableAt: availableAt, Error: Describe(error));
    }

    private async Task<DispatchOutcome> DeadLetterAsync(OutboxMessage message, int attempts, Exception error, CancellationToken cancellationToken)
    {
        LogDeadLettered(error, message.Id, message.Destination, attempts);
        TwinboxDiagnostics.MessagesDeadLettered.Add(1, new KeyValuePair<string, object?>("destination", message.Destination));

        foreach (var observer in _observers)
        {
            try
            {
                await observer.OnDeadLetteredAsync(message, error, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception observerError) when (!cancellationToken.IsCancellationRequested)
            {
                LogObserverFailed(observerError, observer.GetType().Name, message.Id);
            }
        }

        return new DispatchOutcome(message.Id, OutboxMessageStatus.Dead, attempts, Error: Describe(error));
    }

    private static void RecordDelivered(OutboxMessage message, DateTimeOffset sentAt)
    {
        var tag = new KeyValuePair<string, object?>("destination", message.Destination);
        TwinboxDiagnostics.MessagesSent.Add(1, tag);
        TwinboxDiagnostics.DeliveryLatency.Record((sentAt - message.CreatedAt).TotalMilliseconds, tag);
    }

    private static string Describe(Exception error)
    {
        var text = $"{error.GetType().Name}: {error.Message}";
        return text.Length <= MaxErrorLength ? text : text[..MaxErrorLength];
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Sending message {MessageId} to {Destination} failed (attempt {Attempt}); retrying at {RetryAt}.")]
    private partial void LogSendFailed(Exception error, Guid messageId, string destination, int attempt, DateTimeOffset retryAt);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} to {Destination} was dead-lettered after {Attempts} attempt(s).")]
    private partial void LogDeadLettered(Exception error, Guid messageId, string destination, int attempts);

    [LoggerMessage(Level = LogLevel.Error, Message = "Dead-letter observer {Observer} failed for message {MessageId}.")]
    private partial void LogObserverFailed(Exception error, string observer, Guid messageId);
}
