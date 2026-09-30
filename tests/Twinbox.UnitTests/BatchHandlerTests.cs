using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class BatchHandlerTests
{
    [Fact]
    public async Task ProcessBatch_GivesTheBatchHandlerEveryNewMessageAtOnce()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal);
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await pipeline.ProcessBatchAsync([Incoming("a", 1), Incoming("b", 2), Incoming("c", 3)], default);

        Assert.Equal(["batch:1,2,3"], journal.Entries);
    }

    [Fact]
    public async Task ProcessBatch_FiltersOutAlreadyProcessedMessages()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal);
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await pipeline.ProcessBatchAsync([Incoming("a", 1), Incoming("b", 2)], default);
        await pipeline.ProcessBatchAsync([Incoming("b", 2), Incoming("c", 3), Incoming("a", 1)], default);
        await pipeline.ProcessBatchAsync([Incoming("a", 1)], default);

        Assert.Equal(["batch:1,2", "batch:3"], journal.Entries);
    }

    [Fact]
    public async Task SingleMessageTransports_DeliverBatchesOfOne()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal);

        await host.SendAsync(o =>
        {
            o.Send(new OrderPlaced(4));
            o.Send(new OrderPlaced(5));
        });
        await host.Harness.DrainAsync();

        Assert.Equal(["batch:4", "batch:5"], journal.Entries);
    }

    [Fact]
    public async Task ProcessBatch_StillRunsSingleMessageHandlersPerMessage()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal, b => b.AddHandler<InboxTests.OrderPlacedHandler, OrderPlaced>());
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await pipeline.ProcessBatchAsync([Incoming("a", 1), Incoming("b", 2)], default);

        Assert.Equal(["OrderPlacedHandler:1", "OrderPlacedHandler:2", "batch:1,2"], journal.Entries.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task FailedBatch_RollsBackSoItCanBeRetried()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal, b => b.AddBatchHandler<FailOnceBatchHandler, OrderPlaced>());
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.ProcessBatchAsync([Incoming("a", 1), Incoming("b", 2)], default));
        await pipeline.ProcessBatchAsync([Incoming("a", 1), Incoming("b", 2)], default);

        Assert.Contains("retried:1,2", journal.Entries);
    }

    [Fact]
    public async Task BatchFilters_WrapBatchesFromABatchInbox()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal, b => b.AddBatchFilter<RecordingBatchFilter>().AddFilter<RecordingFilter>());
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await pipeline.ProcessBatchAsync([Incoming("a", 1), Incoming("b", 2)], default);

        Assert.Equal(["batch-filter:before:a,b", "batch:1,2", "batch-filter:after"], journal.Entries);
    }

    [Fact]
    public async Task BatchFilters_WrapBatchesOfOneFromSingleMessageTransports()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal, b => b.AddBatchFilter<RecordingBatchFilter>().AddFilter<RecordingFilter>());

        await host.SendAsync(o => o.Send(new OrderPlaced(4)));
        await host.Harness.DrainAsync();

        var id = Assert.Single(host.Harness.Transport.Sent).MessageId;
        Assert.Equal([$"batch-filter:before:{id}", "batch:4", "batch-filter:after"], journal.Entries);
    }

    [Fact]
    public async Task BatchFilters_WrapBatchesWithTheInboxTurnedOff()
    {
        var journal = new InboxTests.Journal();
        await using var host = CreateHost(journal, b => b
            .AddBatchFilter<RecordingBatchFilter>()
            .Configure(o => o.Inbox.Enabled = false));
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await pipeline.ProcessBatchAsync([Incoming("a", 1), Incoming("b", 2)], default);

        Assert.Equal(["batch-filter:before:a,b", "batch:1,2", "batch-filter:after"], journal.Entries);
    }

    private static TestHost CreateHost(InboxTests.Journal journal, Action<TwinboxBuilder>? configure = null) =>
        TestHost.Create(
            b =>
            {
                b.Route<OrderPlaced>().To("orders").AddBatchHandler<RecordingBatchHandler, OrderPlaced>();
                configure?.Invoke(b);
            },
            s => s.AddSingleton(journal).AddSingleton<FailureCounter>());

    private static IncomingMessage Incoming(string id, int orderId) =>
        new(id, "OrderPlaced", "orders", Encoding.UTF8.GetBytes($$"""{"orderId":{{orderId}}}"""), "application/json", new Dictionary<string, string>(), 1, null);

    public sealed class RecordingBatchHandler(InboxTests.Journal journal) : IHandleBatch<OrderPlaced>
    {
        public Task HandleAsync(IReadOnlyList<BatchItem<OrderPlaced>> batch, CancellationToken cancellationToken)
        {
            journal.Add("batch:" + string.Join(",", batch.Select(b => b.Message.OrderId)));
            return Task.CompletedTask;
        }
    }

    public sealed class RecordingBatchFilter(InboxTests.Journal journal) : IBatchMessageFilter
    {
        public async Task InvokeAsync(IReadOnlyList<BatchItem<object>> batch, Func<Task> continuation, CancellationToken cancellationToken)
        {
            journal.Add("batch-filter:before:" + string.Join(",", batch.Select(b => b.Context.MessageId)));
            await continuation();
            journal.Add("batch-filter:after");
        }
    }

    public sealed class RecordingFilter(InboxTests.Journal journal) : IMessageFilter
    {
        public Task InvokeAsync(object message, MessageContext context, Func<Task> continuation, CancellationToken cancellationToken)
        {
            journal.Add("filter:" + context.MessageId);
            return continuation();
        }
    }

    public sealed class FailureCounter
    {
        public int Calls { get; set; }
    }

    public sealed class FailOnceBatchHandler(InboxTests.Journal journal, FailureCounter counter) : IHandleBatch<OrderPlaced>
    {
        public Task HandleAsync(IReadOnlyList<BatchItem<OrderPlaced>> batch, CancellationToken cancellationToken)
        {
            if (counter.Calls++ == 0)
            {
                throw new InvalidOperationException("first batch fails");
            }

            journal.Add("retried:" + string.Join(",", batch.Select(b => b.Message.OrderId)));
            return Task.CompletedTask;
        }
    }
}
