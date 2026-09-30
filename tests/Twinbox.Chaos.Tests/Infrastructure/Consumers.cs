using System.Collections.Concurrent;
using Dapper;

namespace Twinbox.Chaos.Tests;

[MessageName("handled")]
public sealed record Handled(string MessageId, string Consumer);

/// <summary>The statement consumers record their side effect with, inside the handler transaction.</summary>
public sealed record EffectStatement(string Sql);

/// <summary>How many times each consumer ran for each message, committed or not.</summary>
public sealed class RunCounter
{
    private readonly ConcurrentDictionary<(string MessageId, string Consumer), int> _runs = new();

    public int Next(string messageId, string consumer) => _runs.AddOrUpdate((messageId, consumer), 1, (_, runs) => runs + 1);

    public int Runs(string messageId, string consumer) => _runs.GetValueOrDefault((messageId, consumer));
}

/// <summary>How many times each message's handler ran to completion.</summary>
public sealed class Journal
{
    private readonly ConcurrentDictionary<int, int> _handled = new();

    public int Distinct => _handled.Count;

    public IReadOnlyDictionary<int, int> Handled => _handled;

    public void Record(int id) => _handled.AddOrUpdate(id, 1, (_, count) => count + 1);
}

public sealed class JournalConsumer(Journal journal) : IHandle<Numbered>
{
    public Task HandleAsync(Numbered message, MessageContext context, CancellationToken cancellationToken)
    {
        journal.Record(message.Id);
        return Task.CompletedTask;
    }
}

public sealed class SteadyConsumer(HandlerTransaction transaction, IOutbox outbox, EffectStatement effect, RunCounter runs) : IHandle<Numbered>
{
    public const string Name = "steady";

    public async Task HandleAsync(Numbered message, MessageContext context, CancellationToken cancellationToken)
    {
        runs.Next(context.MessageId, Name);
        await transaction.Connection.ExecuteAsync(effect.Sql, new { messageId = context.MessageId, consumer = Name }, transaction.Transaction);
        outbox.Send(new Handled(context.MessageId, Name));
    }
}

/// <summary>Fails its first run of every message after writing, so that run's effects must roll back.</summary>
public sealed class FailFirstConsumer(HandlerTransaction transaction, IOutbox outbox, EffectStatement effect, RunCounter runs) : IHandle<Numbered>
{
    public const string Name = "fail-first";

    public async Task HandleAsync(Numbered message, MessageContext context, CancellationToken cancellationToken)
    {
        var run = runs.Next(context.MessageId, Name);
        await transaction.Connection.ExecuteAsync(effect.Sql, new { messageId = context.MessageId, consumer = Name }, transaction.Transaction);
        outbox.Send(new Handled(context.MessageId, Name));
        if (run == 1)
        {
            throw new FirstRunException();
        }
    }
}

public sealed class BatchConsumer(HandlerTransaction transaction, EffectStatement effect, RunCounter runs) : IHandleBatch<Numbered>
{
    public const string Name = "batch";

    public async Task HandleAsync(IReadOnlyList<BatchItem<Numbered>> batch, CancellationToken cancellationToken)
    {
        foreach (var item in batch)
        {
            runs.Next(item.Context.MessageId, Name);
            await transaction.Connection.ExecuteAsync(effect.Sql, new { messageId = item.Context.MessageId, consumer = Name }, transaction.Transaction);
        }
    }
}

/// <summary>Holds its inbox transaction open until the other handlers join, which only works if they aren't serialized.</summary>
public sealed class RendezvousConsumer(HandlerTransaction transaction, EffectStatement effect, Rendezvous rendezvous) : IHandle<Numbered>
{
    public const string Name = "rendezvous";

    public async Task HandleAsync(Numbered message, MessageContext context, CancellationToken cancellationToken)
    {
        await transaction.Connection.ExecuteAsync(effect.Sql, new { messageId = context.MessageId, consumer = Name }, transaction.Transaction);
        await rendezvous.ArriveAsync();
    }
}

/// <summary>Lets handlers wait until <paramref name="expected"/> of them are running at the same time.</summary>
public sealed class Rendezvous(int expected)
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private readonly TaskCompletionSource _everyone = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _arrived;

    public Task ArriveAsync()
    {
        if (Interlocked.Increment(ref _arrived) >= expected)
        {
            _everyone.TrySetResult();
        }

        return _everyone.Task.WaitAsync(Patience);
    }
}

public sealed class FirstRunException() : Exception("The first run fails after writing its side effects.");
