using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class InboxTests
{
    [Fact]
    public async Task Message_IsDeliveredToItsHandler()
    {
        var journal = new Journal();
        await using var host = CreateHost(journal, b => b.AddHandler<OrderPlacedHandler, OrderPlaced>());

        await host.SendAsync(o => o.Send(new OrderPlaced(5)));
        await host.Harness.DrainAsync();

        Assert.Equal(["OrderPlacedHandler:5"], journal.Entries);
    }

    [Fact]
    public async Task RedeliveredMessage_IsProcessedOnce()
    {
        var journal = new Journal();
        await using var host = CreateHost(journal, b => b.AddHandler<OrderPlacedHandler, OrderPlaced>());
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();
        var incoming = Incoming("msg-1", "OrderPlaced", """{"orderId":9}""");

        await pipeline.ProcessAsync(incoming, default);
        await pipeline.ProcessAsync(incoming with { DeliveryAttempt = 2 }, default);

        Assert.Equal(["OrderPlacedHandler:9"], journal.Entries);
    }

    [Fact]
    public async Task EachHandler_IsDeduplicatedIndependently()
    {
        var journal = new Journal();
        await using var host = CreateHost(journal, b => b
            .AddHandler<OrderPlacedHandler, OrderPlaced>()
            .AddHandler<FlakyOrderPlacedHandler, OrderPlaced>());
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();
        var incoming = Incoming("msg-2", "OrderPlaced", """{"orderId":3}""");

        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.ProcessAsync(incoming, default));
        await pipeline.ProcessAsync(incoming with { DeliveryAttempt = 2 }, default);

        Assert.Equal(["OrderPlacedHandler:3", "FlakyOrderPlacedHandler:3"], journal.Entries);
    }

    [Fact]
    public async Task FailedHandler_DiscardsMessagesItSent()
    {
        var journal = new Journal();
        await using var host = CreateHost(journal, b => b
            .Route<OrderShipped>().To("shipping")
            .AddHandler<ShipThenFailHandler, OrderPlaced>());
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => pipeline.ProcessAsync(Incoming("msg-3", "OrderPlaced", """{"orderId":1}"""), default));
        await host.Harness.DrainAsync();

        Assert.Empty(host.Harness.Sent<OrderShipped>());
    }

    [Fact]
    public async Task Handler_CanSendFollowUpMessages()
    {
        var journal = new Journal();
        await using var host = CreateHost(journal, b => b
            .Route<OrderShipped>().To("shipping")
            .AddHandler<ShipOrderHandler, OrderPlaced>());

        await host.SendAsync(o => o.Send(new OrderPlaced(11)));
        await host.Harness.DrainAsync();

        Assert.Equal(new OrderShipped(11), Assert.Single(host.Harness.Sent<OrderShipped>()));
    }

    [Fact]
    public async Task UnknownMessage_IsDeadLetteredByDefault()
    {
        await using var host = CreateHost(new Journal(), _ => { });
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => pipeline.ProcessAsync(Incoming("msg-4", "something-else", "{}"), default));
    }

    [Fact]
    public async Task UnknownMessage_CanBeIgnored()
    {
        await using var host = CreateHost(new Journal(), b => b.Configure(o => o.Inbox.UnknownMessages = UnknownMessagePolicy.Ignore));
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await pipeline.ProcessAsync(Incoming("msg-5", "something-else", "{}"), default);
    }

    [Fact]
    public async Task MalformedBody_IsPermanentFailure()
    {
        await using var host = CreateHost(new Journal(), b => b.AddHandler<OrderPlacedHandler, OrderPlaced>());
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => pipeline.ProcessAsync(Incoming("msg-6", "OrderPlaced", "not json"), default));
    }

    [Fact]
    public async Task ReflectionRegistration_FindsEveryHandledMessage()
    {
        var journal = new Journal();
        await using var host = CreateHost(journal, b => b.AddHandler<MultiHandler>());
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await pipeline.ProcessAsync(Incoming("a", "OrderPlaced", """{"orderId":1}"""), default);
        await pipeline.ProcessAsync(Incoming("b", "OrderShipped", """{"orderId":2}"""), default);

        Assert.Equal(["placed:1", "shipped:2"], journal.Entries);
    }

    private static TestHost CreateHost(Journal journal, Action<TwinboxBuilder> configure) =>
        TestHost.Create(
            b =>
            {
                b.Route<OrderPlaced>().To("orders");
                configure(b);
            },
            s => s.AddSingleton(journal));

    private static IncomingMessage Incoming(string id, string name, string json) =>
        new(id, name, "orders", Encoding.UTF8.GetBytes(json), "application/json", new Dictionary<string, string>(), 1, null);

    public sealed class Journal
    {
        private readonly List<string> _entries = [];

        public IReadOnlyList<string> Entries
        {
            get
            {
                lock (_entries)
                {
                    return [.. _entries];
                }
            }
        }

        public void Add(string entry)
        {
            lock (_entries)
            {
                _entries.Add(entry);
            }
        }
    }

    public sealed class OrderPlacedHandler(Journal journal) : IHandle<OrderPlaced>
    {
        public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
        {
            journal.Add($"{nameof(OrderPlacedHandler)}:{message.OrderId}");
            return Task.CompletedTask;
        }
    }

    public sealed class FlakyOrderPlacedHandler(Journal journal) : IHandle<OrderPlaced>
    {
        public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
        {
            if (context.DeliveryAttempt == 1)
            {
                throw new InvalidOperationException("first attempt fails");
            }

            journal.Add($"{nameof(FlakyOrderPlacedHandler)}:{message.OrderId}");
            return Task.CompletedTask;
        }
    }

    public sealed class ShipOrderHandler(IOutbox outbox) : IHandle<OrderPlaced>
    {
        public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
        {
            outbox.Send(new OrderShipped(message.OrderId));
            return Task.CompletedTask;
        }
    }

    public sealed class ShipThenFailHandler(IOutbox outbox) : IHandle<OrderPlaced>
    {
        public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
        {
            outbox.Send(new OrderShipped(message.OrderId));
            throw new InvalidOperationException("failed after sending");
        }
    }

    public sealed class MultiHandler(Journal journal) : IHandle<OrderPlaced>, IHandle<OrderShipped>
    {
        public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken)
        {
            journal.Add($"placed:{message.OrderId}");
            return Task.CompletedTask;
        }

        public Task HandleAsync(OrderShipped message, MessageContext context, CancellationToken cancellationToken)
        {
            journal.Add($"shipped:{message.OrderId}");
            return Task.CompletedTask;
        }
    }
}
