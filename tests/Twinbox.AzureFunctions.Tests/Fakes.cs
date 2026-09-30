using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Time.Testing;
using Twinbox.Transport;

namespace Twinbox.AzureFunctions.Tests;

public sealed record OrderPlaced(int OrderId);

internal sealed record Settled(string Action, string MessageId, string? Reason = null, string? Description = null);

internal sealed class RecordingMessageActions : ServiceBusMessageActions
{
    public List<Settled> Settlements { get; } = [];

    public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
    {
        Settlements.Add(new Settled("complete", message.MessageId));
        return Task.CompletedTask;
    }

    public override Task AbandonMessageAsync(
        ServiceBusReceivedMessage message,
        IDictionary<string, object>? propertiesToModify = null,
        CancellationToken cancellationToken = default)
    {
        Settlements.Add(new Settled("abandon", message.MessageId));
        return Task.CompletedTask;
    }

    public override Task DeadLetterMessageAsync(
        ServiceBusReceivedMessage message,
        Dictionary<string, object>? propertiesToModify = null,
        string? deadLetterReason = null,
        string? deadLetterErrorDescription = null,
        CancellationToken cancellationToken = default)
    {
        Settlements.Add(new Settled("deadletter", message.MessageId, deadLetterReason, deadLetterErrorDescription));
        return Task.CompletedTask;
    }
}

internal sealed class RecordingPipeline(Func<IncomingMessage, Task>? onProcess = null) : IInboundPipeline
{
    public List<IncomingMessage> Received { get; } = [];

    public Task ProcessAsync(IncomingMessage message, CancellationToken cancellationToken)
    {
        Received.Add(message);
        return onProcess?.Invoke(message) ?? Task.CompletedTask;
    }
}

internal sealed class ScriptedDispatcher(FakeTimeProvider time, TimeSpan batchDuration, Func<int, int> claimedOnCall) : IOutboxDispatcher
{
    public int Calls { get; private set; }

    public Task<int> DispatchBatchAsync(CancellationToken cancellationToken)
    {
        time.Advance(batchDuration);
        return Task.FromResult(claimedOnCall(Calls++));
    }
}

internal sealed class Journal
{
    private readonly List<string> _entries = [];

    public IReadOnlyList<string> Entries => _entries;

    public void Add(string entry) => _entries.Add(entry);
}

internal sealed class OrderPlacedHandler(Journal journal) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
    {
        journal.Add($"{context.MessageId}:{message.OrderId}");
        return Task.CompletedTask;
    }
}
