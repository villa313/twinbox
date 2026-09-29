using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class OutboxTests
{
    [Fact]
    public async Task Send_ThenCommit_DeliversToRoutedDestination()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));

        await host.SendAsync(o => o.Send(new OrderPlaced(42)));
        await host.Harness.DrainAsync();

        var sent = Assert.Single(host.Harness.Transport.Sent);
        Assert.Equal("orders", sent.Destination);
        Assert.Equal("OrderPlaced", sent.MessageName);
        Assert.Equal(new OrderPlaced(42), Assert.Single(host.Harness.Sent<OrderPlaced>()));
    }

    [Fact]
    public async Task Send_AsBaseType_RoutesAndSerializesTheRuntimeType()
    {
        await using var host = TestHost.Create(b => b.Route<CustomerRegistered>().To("customers"));
        DomainEvent domainEvent = new CustomerRegistered(5);

        await host.SendAsync(o => o.Send(domainEvent));
        await host.Harness.DrainAsync();

        Assert.Equal("customers", Assert.Single(host.Harness.Transport.Sent).Destination);
        Assert.Equal(new CustomerRegistered(5), Assert.Single(host.Harness.Sent<CustomerRegistered>()));
    }

    [Fact]
    public async Task Send_WithoutCommit_IsNeverDelivered()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));

        await using (var scope = host.Services.CreateAsyncScope())
        {
            scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(1));
        }

        await host.Harness.DrainAsync();

        Assert.Empty(host.Harness.Transport.Sent);
    }

    [Fact]
    public async Task Send_UnroutedMessage_ThrowsWithHowToFix()
    {
        await using var host = TestHost.Create(_ => { });
        await using var scope = host.Services.CreateAsyncScope();

        var error = Assert.Throws<InvalidOperationException>(
            () => scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(1)));

        Assert.Contains("Route<OrderPlaced>()", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Send_MultipleRoutes_FansOut()
    {
        await using var host = TestHost.Create(b => b
            .Route<OrderPlaced>().To("billing")
            .Route<OrderPlaced>().To("shipping"));

        await host.SendAsync(o => o.Send(new OrderPlaced(7)));
        await host.Harness.DrainAsync();

        Assert.Equal(["billing", "shipping"], host.Harness.Transport.Sent.Select(m => m.Destination).Order());
    }

    [Fact]
    public async Task Send_WithDelay_WaitsUntilDue()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));

        await host.SendAsync(o => o.Send(new OrderPlaced(1), new SendOptions { Delay = TimeSpan.FromMinutes(10) }));
        await host.Harness.DrainAsync();
        Assert.Empty(host.Harness.Transport.Sent);

        host.Time.Advance(TimeSpan.FromMinutes(10));
        await host.Harness.DrainAsync();
        Assert.Single(host.Harness.Transport.Sent);
    }

    [Fact]
    public async Task Send_SamePartition_DeliversInOrderOneAtATime()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));
        var partition = new SendOptions { PartitionKey = "customer-1" };

        await host.SendAsync(o =>
        {
            o.Send(new OrderPlaced(1), partition);
            o.Send(new OrderPlaced(2), partition);
            o.Send(new OrderPlaced(3), partition);
        });

        var dispatcher = host.Services.GetRequiredService<IOutboxDispatcher>();
        Assert.Equal(1, await dispatcher.DispatchBatchAsync(default));
        await host.Harness.DrainAsync();

        Assert.Equal([1, 2, 3], host.Harness.Sent<OrderPlaced>().Select(m => m.OrderId));
    }

    [Fact]
    public async Task Send_CopiesHeadersAndPartitionKeyToTransport()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));

        await host.SendAsync(o => o.Send(
            new OrderPlaced(1),
            new SendOptions { PartitionKey = "p-1", Headers = new Dictionary<string, string> { ["x-tenant"] = "acme" } }));
        await host.Harness.DrainAsync();

        var sent = Assert.Single(host.Harness.Transport.Sent);
        Assert.Equal("p-1", sent.PartitionKey);
        Assert.Equal("acme", sent.Headers["x-tenant"]);
    }

    [Fact]
    public async Task Send_InsideActivity_PropagatesTraceContext()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        using var source = new ActivitySource("test");
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));

        string traceId;
        using (var activity = source.StartActivity("request"))
        {
            traceId = activity!.TraceId.ToString();
            await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        }

        await host.Harness.DrainAsync();

        var sent = Assert.Single(host.Harness.Transport.Sent);
        Assert.Contains(traceId, sent.Headers[TransportHeaders.TraceParent], StringComparison.Ordinal);
    }

    [Fact]
    public async Task Send_TwoTransportsWithoutNamingOne_Throws()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders"),
            s => s.AddSingleton<ITransport>(new NamedTransport("second")));
        await using var scope = host.Services.CreateAsyncScope();

        var error = Assert.Throws<InvalidOperationException>(
            () => scope.ServiceProvider.GetRequiredService<IOutbox>().Send(new OrderPlaced(1)));

        Assert.Contains("transport:", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Store_RecordsSentStatus()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        var row = Assert.Single(host.Harness.Store.Snapshot());
        Assert.Equal(OutboxMessageStatus.Sent, row.Status);
        Assert.Equal(1, row.Attempts);
        Assert.Equal(host.Time.GetUtcNow(), row.SentAt);
    }

    private sealed class NamedTransport(string name) : ITransport
    {
        public string Name => name;

        public Task SendAsync(TransportMessage message, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
