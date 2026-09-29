using System.Diagnostics.Metrics;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Twinbox.Diagnostics;
using Twinbox.InMemory;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus.Tests.Integration;

[Trait("Category", "Integration")]
public sealed class EmulatorTests(ServiceBusEmulatorFixture emulator) : IClassFixture<ServiceBusEmulatorFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task OutboxMessage_IsHandledOnce_AndRedeliveredDuplicateIsSkipped()
    {
        using var duplicates = new CounterProbe("twinbox.inbox.duplicates");
        var journal = new Journal();
        using var host = CreateHost(journal, b => b
            .UseAzureServiceBus(emulator.ConnectionString, o => o.Listen("orders"))
            .Route<OrderPlaced>().To("orders")
            .AddHandler<OrderPlacedHandler, OrderPlaced>());
        await host.StartAsync(TestContext.Current.CancellationToken);

        await SendAsync(host, new OrderPlaced(7));
        await WaitUntilAsync(() => journal.Entries.Count == 1);

        // Re-sending the stored row simulates the dispatcher retrying after a lost broker acknowledgement.
        var sent = Assert.Single(host.Services.GetRequiredService<InMemoryOutboxStore>().Snapshot());
        Assert.Equal(OutboxMessageStatus.Sent, sent.Status);
        await host.Services.GetRequiredService<AzureServiceBusTransport>().SendAsync(ToTransportMessage(sent), default);
        await WaitUntilAsync(() => duplicates.Value >= 1);

        Assert.Equal([$"7:{sent.Id}"], journal.Entries);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task TopicSubscription_DeliversToHandler()
    {
        var journal = new Journal();
        using var host = CreateHost(journal, b => b
            .UseAzureServiceBus(emulator.ConnectionString, o => o.Listen("billing", "invoices"))
            .Route<OrderPlaced>().To("billing")
            .AddHandler<OrderPlacedHandler, OrderPlaced>());
        await host.StartAsync(TestContext.Current.CancellationToken);

        await SendAsync(host, new OrderPlaced(8));
        await WaitUntilAsync(() => journal.Entries.Count == 1);

        Assert.StartsWith("8:", journal.Entries[0], StringComparison.Ordinal);
        Assert.Equal("billing/Subscriptions/invoices", journal.Sources.Single());
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task UnhandleableMessage_IsDeadLetteredWithReason()
    {
        var journal = new Journal();
        using var host = CreateHost(journal, b => b
            .UseAzureServiceBus(emulator.ConnectionString, o => o.Listen("poison"))
            .AddHandler<OrderPlacedHandler, OrderPlaced>());
        await host.StartAsync(TestContext.Current.CancellationToken);
        var transport = host.Services.GetRequiredService<AzureServiceBusTransport>();

        await transport.SendAsync(
            new TransportMessage("poison-1", "nobody-handles-this", "poison", Encoding.UTF8.GetBytes("{}"), "application/json", new Dictionary<string, string>(), null),
            default);

        await using var receiver = transport.Client.CreateReceiver("poison", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var deadLettered = await receiver.ReceiveMessageAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.NotNull(deadLettered);
        Assert.Equal("poison-1", deadLettered.MessageId);
        Assert.Equal(AzureServiceBusMessageHandler.PermanentFailureReason, deadLettered.DeadLetterReason);
        Assert.Contains("nobody-handles-this", deadLettered.DeadLetterErrorDescription, StringComparison.Ordinal);
        await receiver.CompleteMessageAsync(deadLettered, TestContext.Current.CancellationToken);
        Assert.Empty(journal.Entries);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    private static IHost CreateHost(Journal journal, Action<TwinboxBuilder> configure)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddLogging();
        builder.Services.AddSingleton(journal);
        builder.Services.AddTwinbox(b => configure(b.UseInMemoryStore()));
        return builder.Build();
    }

    private static async Task SendAsync(IHost host, OrderPlaced message)
    {
        await using var scope = host.Services.CreateAsyncScope();
        scope.ServiceProvider.GetRequiredService<IOutbox>().Send(message);
        await scope.ServiceProvider.GetRequiredService<InMemoryUnitOfWork>().CommitAsync();
    }

    private static TransportMessage ToTransportMessage(OutboxMessage message) => new(
        message.Id.ToString(),
        message.MessageName,
        message.Destination,
        message.Payload,
        message.ContentType,
        message.Headers,
        message.PartitionKey);

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(Timeout);
        while (!condition())
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), timeout.Token);
        }
    }

    public sealed class Journal
    {
        private readonly List<(string Entry, string Source)> _entries = [];

        public IReadOnlyList<string> Entries
        {
            get
            {
                lock (_entries)
                {
                    return [.. _entries.Select(e => e.Entry)];
                }
            }
        }

        public IReadOnlyList<string> Sources
        {
            get
            {
                lock (_entries)
                {
                    return [.. _entries.Select(e => e.Source)];
                }
            }
        }

        public void Add(string entry, string source)
        {
            lock (_entries)
            {
                _entries.Add((entry, source));
            }
        }
    }

    public sealed class OrderPlacedHandler(Journal journal) : IHandle<OrderPlaced>
    {
        public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
        {
            journal.Add($"{message.OrderId}:{context.MessageId}", context.Source);
            return Task.CompletedTask;
        }
    }

    private sealed class CounterProbe : IDisposable
    {
        private readonly MeterListener _listener = new();
        private long _value;

        public CounterProbe(string instrumentName)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == TwinboxDiagnostics.SourceName && instrument.Name == instrumentName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => Interlocked.Add(ref _value, measurement));
            _listener.Start();
        }

        public long Value => Interlocked.Read(ref _value);

        public void Dispose() => _listener.Dispose();
    }
}
