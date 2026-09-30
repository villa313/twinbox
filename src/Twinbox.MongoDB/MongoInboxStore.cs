using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Twinbox.Storage;

namespace Twinbox.MongoDB;

internal sealed partial class MongoInboxStore(
    MongoSettings settings,
    MongoOutboxStore store,
    IDispatchSignal signal,
    ILogger<MongoInboxStore> logger) : IInboxStore
{
    public const string ProcessedAt = "ProcessedAt";

    private const string TransientTransactionError = "TransientTransactionError";
    private const string UnknownTransactionCommitResult = "UnknownTransactionCommitResult";

    // Matches the driver's WithTransaction budget.
    private static readonly TimeSpan RetryBudget = TimeSpan.FromMinutes(2);

    private readonly ILogger _logger = logger;

    public async Task<bool> TryProcessAsync(
        InboxEntry entry,
        IServiceProvider scopedServices,
        Func<CancellationToken, Task> handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(scopedServices);
        ArgumentNullException.ThrowIfNull(handler);

        var outboxSession = scopedServices.GetRequiredService<IOutboxSession>();
        var handlerSession = scopedServices.GetRequiredService<MongoDBHandlerSession>();

        await using var lease = await store.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var session = await lease.Database.Client.StartSessionAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        var started = Stopwatch.GetTimestamp();

        for (var attempt = 1; ; attempt++)
        {
            IReadOnlyList<OutboxMessage> pending;
            session.StartTransaction();
            handlerSession.Attach(session, lease.Database);
            try
            {
                if (!await TryInsertEntryAsync(lease.Database, session, entry, cancellationToken).ConfigureAwait(false))
                {
                    await AbortAsync(session).ConfigureAwait(false);
                    LogDuplicate(entry.MessageId, entry.Consumer);
                    return false;
                }

                await handler(cancellationToken).ConfigureAwait(false);
                pending = outboxSession.TakePending();
                await store.InsertAsync(lease.Database, session, pending, cancellationToken).ConfigureAwait(false);
                await CommitAsync(session, started, cancellationToken).ConfigureAwait(false);
            }
            catch (MongoException ex) when (ex.HasErrorLabel(TransientTransactionError) && WithinBudget(started))
            {
                await AbortAsync(session).ConfigureAwait(false);
                outboxSession.TakePending();
                LogRetrying(entry.MessageId, entry.Consumer, attempt, ex.Message);
                await Task.Delay(Backoff(attempt), cancellationToken).ConfigureAwait(false);
                continue;
            }
            catch
            {
                await AbortAsync(session).ConfigureAwait(false);
                outboxSession.TakePending();
                throw;
            }
            finally
            {
                handlerSession.Detach();
            }

            if (pending.Count > 0)
            {
                signal.Notify();
            }

            return true;
        }
    }

    public async Task<int> PurgeAsync(DateTimeOffset processedBefore, int batchSize, CancellationToken cancellationToken)
    {
        await using var lease = await store.OpenAsync(cancellationToken).ConfigureAwait(false);
        return await MongoOutboxStore.DeleteBatchAsync(
            settings.Inbox(lease.Database),
            new BsonDocument(ProcessedAt, new BsonDocument("$lt", OutboxDocument.Timestamp(processedBefore))),
            batchSize,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The compound _id keeps entries unique even when the app manages indexes itself.</summary>
    private async Task<bool> TryInsertEntryAsync(IMongoDatabase database, IClientSessionHandle session, InboxEntry entry, CancellationToken cancellationToken)
    {
        var document = new BsonDocument
        {
            { "_id", new BsonDocument { { "MessageId", entry.MessageId }, { "Consumer", entry.Consumer } } },
            { "Source", entry.Source },
            { ProcessedAt, OutboxDocument.Timestamp(entry.ReceivedAt) },
        };
        try
        {
            await settings.Inbox(database).InsertOneAsync(session, document, cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
        {
            return false;
        }
    }

    private async Task CommitAsync(IClientSessionHandle session, long started, CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                await session.CommitTransactionAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (MongoException ex) when (ex.HasErrorLabel(UnknownTransactionCommitResult) && WithinBudget(started))
            {
                LogCommitRetrying(ex.Message);
            }
        }
    }

    // The driver ignores server errors on abort; a failed commit already ended the transaction.
    private static async Task AbortAsync(IClientSessionHandle session)
    {
        if (session.IsInTransaction)
        {
            await session.AbortTransactionAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static bool WithinBudget(long started) => Stopwatch.GetElapsedTime(started) < RetryBudget;

    // Write conflicts with a concurrent duplicate fail fast instead of waiting, so back off before retrying.
    private static TimeSpan Backoff(int attempt) => TimeSpan.FromMilliseconds(Math.Min(10 * attempt, 250));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Message {MessageId} was already processed by {Consumer}; skipping")]
    private partial void LogDuplicate(string messageId, string consumer);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Retrying the inbox transaction for message {MessageId} and {Consumer} after attempt {Attempt}: {Reason}")]
    private partial void LogRetrying(string messageId, string consumer, int attempt, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Retrying an inbox transaction commit with an unknown result: {Reason}")]
    private partial void LogCommitRetrying(string reason);
}
