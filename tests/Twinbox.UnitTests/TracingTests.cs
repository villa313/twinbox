using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Diagnostics;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

[Collection(nameof(TracingTests))]
public sealed class TracingTests : IDisposable
{
    private readonly ConcurrentQueue<Activity> _stopped = new();
    private readonly ActivityListener _listener;
    private readonly ActivitySource _request = new("tracing-tests");

    public TracingTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is TwinboxDiagnostics.SourceName or "tracing-tests",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = _stopped.Enqueue,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    [Fact]
    public async Task Send_RecordsACreateSpan_UnderTheRequest()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));

        var request = await SendInRequestAsync(host, new OrderPlaced(1));

        var create = Single("create orders");
        Assert.Equal(ActivityKind.Producer, create.Kind);
        Assert.Equal(request.SpanId, create.ParentSpanId);
        Assert.Equal("create", create.GetTagItem("messaging.operation.type"));
        Assert.Equal("orders", create.GetTagItem("messaging.destination.name"));
    }

    [Fact]
    public async Task Dispatch_RecordsASendSpan_ChildOfCreate_WithSemanticConventionTags()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));
        await SendInRequestAsync(host, new OrderPlaced(1), new SendOptions { CorrelationId = "conv-7" });

        await host.Harness.DrainAsync();

        var send = Single("send orders");
        Assert.Equal(ActivityKind.Producer, send.Kind);
        Assert.Equal(Single("create orders").SpanId, send.ParentSpanId);
        Assert.Equal("send", send.GetTagItem("messaging.operation.type"));
        Assert.Equal("send", send.GetTagItem("messaging.operation.name"));
        Assert.Equal("orders", send.GetTagItem("messaging.destination.name"));
        Assert.Equal("conv-7", send.GetTagItem("messaging.message.conversation_id"));
        Assert.Equal(1, send.GetTagItem("twinbox.delivery_attempt"));
    }

    [Fact]
    public async Task Delivery_RecordsAProcessSpan_ChildOfSend()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders").AddHandler<InboxTests.OrderPlacedHandler, OrderPlaced>(),
            s => s.AddSingleton(new InboxTests.Journal()));
        await SendInRequestAsync(host, new OrderPlaced(1));

        await host.Harness.DrainAsync();

        var process = Single("process orders");
        Assert.Equal(ActivityKind.Consumer, process.Kind);
        Assert.Equal(Single("send orders").SpanId, process.ParentSpanId);
        Assert.Equal("process", process.GetTagItem("messaging.operation.type"));
        Assert.Equal("orders", process.GetTagItem("messaging.destination.name"));
        Assert.Equal(ActivityStatusCode.Unset, process.Status);
    }

    [Fact]
    public async Task FailedSend_MarksTheSendSpanAsAnError()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));
        host.Harness.Transport.OnSend = _ => throw new TimeoutException("broker down");
        await SendInRequestAsync(host, new OrderPlaced(1));

        await host.Services.GetRequiredService<IOutboxDispatcher>().DispatchBatchAsync(default);

        AssertRecordedError(Single("send orders"), typeof(TimeoutException), "broker down");
    }

    [Fact]
    public async Task FailingHandler_MarksTheProcessSpanAsAnError()
    {
        await using var host = TestHost.Create(b => b.AddHandler<ThrowingHandler, OrderPlaced>());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            host.Services.GetRequiredService<IInboundPipeline>().ProcessAsync(Incoming("m-1", 1), default));

        AssertRecordedError(Single("process orders"), typeof(InvalidOperationException), "handler broke");
    }

    [Fact]
    public async Task TraceStateAndBaggage_TravelWithTheMessage()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders").AddHandler<InboxTests.OrderPlacedHandler, OrderPlaced>(),
            s => s.AddSingleton(new InboxTests.Journal()));

        using (var request = _request.StartActivity("request")!)
        {
            request.TraceStateString = "vendor=abc";
            request.AddBaggage("tenant", "acme corp");
            await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        }

        await host.Harness.DrainAsync();

        var sent = Assert.Single(host.Harness.Transport.Sent);
        Assert.Equal("vendor=abc", sent.Headers[TransportHeaders.TraceState]);
        Assert.Equal("tenant=acme%20corp", sent.Headers[TransportHeaders.Baggage]);
        var process = Single("process orders");
        Assert.Equal("vendor=abc", process.TraceStateString);
        Assert.Equal("acme corp", process.GetBaggageItem("tenant"));
    }

    [Fact]
    public async Task BatchDelivery_RecordsOneProcessSpan_LinkedToEachMessage()
    {
        await using var host = TestHost.Create(
            b => b.AddBatchHandler<BatchHandlerTests.RecordingBatchHandler, OrderPlaced>(),
            s => s.AddSingleton(new InboxTests.Journal()));
        var first = ActivityContext.Parse("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", null);
        var second = ActivityContext.Parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", null);

        await host.Services.GetRequiredService<IInboundPipeline>().ProcessBatchAsync(
            [Incoming("a", 1, first), Incoming("b", 2, second)], default);

        var process = Single("process orders");
        Assert.Equal(ActivityKind.Consumer, process.Kind);
        Assert.Equal(2, process.GetTagItem("messaging.batch.message_count"));
        Assert.Equal([first.TraceId, second.TraceId], process.Links.Select(l => l.Context.TraceId));
    }

    public void Dispose()
    {
        _listener.Dispose();
        _request.Dispose();
    }

    private static void AssertRecordedError(Activity span, Type exceptionType, string message)
    {
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(exceptionType.FullName, span.GetTagItem("error.type"));
        var exception = Assert.Single(span.Events, e => e.Name == "exception");
        Assert.Equal(message, exception.Tags.Single(t => t.Key == "exception.message").Value);
    }

    private async Task<Activity> SendInRequestAsync(TestHost host, OrderPlaced message, SendOptions? options = null)
    {
        using var request = _request.StartActivity("request")!;
        await host.SendAsync(o => o.Send(message, options));
        return request;
    }

    private Activity Single(string name) => Assert.Single(_stopped, a => a.DisplayName == name);

    private static IncomingMessage Incoming(string id, int orderId, ActivityContext? parent = null)
    {
        var headers = new Dictionary<string, string>();
        if (parent is { } context)
        {
            headers[TransportHeaders.TraceParent] = $"00-{context.TraceId}-{context.SpanId}-01";
        }

        return new IncomingMessage(id, "OrderPlaced", "orders", Encoding.UTF8.GetBytes($$"""{"orderId":{{orderId}}}"""), "application/json", headers, 1, null);
    }

    public sealed class ThrowingHandler : IHandle<OrderPlaced>
    {
        public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("handler broke");
    }
}

[CollectionDefinition(nameof(TracingTests), DisableParallelization = true)]
public sealed class TracingTestsDefinition;
