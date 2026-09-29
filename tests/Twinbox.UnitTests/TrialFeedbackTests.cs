using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Twinbox.Inbox;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class TrialFeedbackTests
{
    [Fact]
    public void ConsumerName_ForGenericHandler_HasNoAssemblyVersions()
    {
        var name = ConsumerNames.For(typeof(EventRelay<CustomerRegistered>));

        Assert.Equal("Twinbox.UnitTests.TrialFeedbackTests+EventRelay<Twinbox.UnitTests.CustomerRegistered>", name);
    }

    [Fact]
    public async Task BaseTypeRoute_CoversSubtypes()
    {
        await using var host = TestHost.Create(b => b.Route<DomainEvent>().To("domain-events"));

        await host.SendAsync(o => o.Send(new CustomerRegistered(1)));
        await host.Harness.DrainAsync();

        var sent = Assert.Single(host.Harness.Transport.Sent);
        Assert.Equal("domain-events", sent.Destination);
        Assert.Equal("CustomerRegistered", sent.MessageName);
    }

    [Fact]
    public async Task InterfaceRoute_CoversImplementations()
    {
        await using var host = TestHost.Create(b => b.Route<IAuditEvent>().To("audit"));

        await host.SendAsync(o => o.Send(new SettingChanged("theme")));
        await host.Harness.DrainAsync();

        Assert.Equal("audit", Assert.Single(host.Harness.Transport.Sent).Destination);
    }

    [Fact]
    public async Task BaseTypeHandler_ReceivesSubtypes()
    {
        var journal = new InboxTests.Journal();
        await using var host = TestHost.Create(
            b => b.Route<CustomerRegistered>().To("customers").AddHandler<EventRelay<DomainEvent>, DomainEvent>(),
            s => s.AddSingleton(journal));

        await host.SendAsync(o => o.Send(new CustomerRegistered(4)));
        await host.Harness.DrainAsync();

        Assert.Equal(["CustomerRegistered { CustomerId = 4 }"], journal.Entries);
    }

    [Fact]
    public async Task KnownTypeWithoutHandler_IsNotAcknowledgedSilently()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders"));
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(() => pipeline.ProcessAsync(
            new IncomingMessage("x", "OrderPlaced", "orders", Encoding.UTF8.GetBytes("""{"orderId":1}"""), "application/json", new Dictionary<string, string>(), 1, null),
            default));

        Assert.Contains("no handler", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddTwinbox_CalledPerModule_CombinesRoutesAndHandlers()
    {
        var journal = new InboxTests.Journal();
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders"),
            s => s.AddSingleton(journal).AddTwinbox(b => b.AddHandler<InboxTests.OrderPlacedHandler, OrderPlaced>()));

        await host.SendAsync(o => o.Send(new OrderPlaced(6)));
        await host.Harness.DrainAsync();

        Assert.Equal(["OrderPlacedHandler:6"], journal.Entries);
    }

    [Fact]
    public async Task DeadLetters_CanReportTheirOwnStatus()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders"),
            s => s.AddHealthChecks().AddTwinbox(deadLetterStatus: HealthStatus.Unhealthy));
        host.Harness.Transport.OnSend = _ => throw new PermanentDeliveryException("bad");

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        var report = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync();
        Assert.Equal(HealthStatus.Unhealthy, report.Status);
    }

    public sealed class EventRelay<TEvent>(InboxTests.Journal journal) : IHandle<TEvent>
        where TEvent : class
    {
        public Task HandleAsync(TEvent message, MessageContext context, CancellationToken cancellationToken)
        {
            journal.Add(message.ToString()!);
            return Task.CompletedTask;
        }
    }
}
