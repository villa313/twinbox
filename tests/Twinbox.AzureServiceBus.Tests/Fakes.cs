using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Twinbox.Transport;

// The SDK mocking constructors leave no connection for base.DisposeAsync to close.
#pragma warning disable CA2215

namespace Twinbox.AzureServiceBus.Tests;

internal sealed class FakeServiceBusClient : ServiceBusClient
{
    private int _sendersCreated;

    public ConcurrentDictionary<string, FakeSender> Senders { get; } = new();

    public int SendersCreated => _sendersCreated;

    public bool Disposed { get; private set; }

    public Func<ServiceBusMessage, Exception?> Failure { get; set; } = _ => null;

    public override ServiceBusSender CreateSender(string queueOrTopicName)
    {
        Interlocked.Increment(ref _sendersCreated);
        return Senders.GetOrAdd(queueOrTopicName, name => new FakeSender(this, name));
    }

    public override ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

internal sealed class FakeSender(FakeServiceBusClient owner, string entityPath) : ServiceBusSender
{
    public override string EntityPath => entityPath;

    public List<ServiceBusMessage> Sent { get; } = [];

    public bool Disposed { get; private set; }

    public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
    {
        if (owner.Failure(message) is { } failure)
        {
            throw failure;
        }

        Sent.Add(message);
        return Task.CompletedTask;
    }

    public override ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
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
