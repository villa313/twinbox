using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Twinbox.Testing;

namespace Twinbox.UnitTests;

public sealed class AdoptionHelpersTests
{
    [Fact]
    public void EnableTwinboxDispatcher_WinsOverSendOnly_InEitherOrder()
    {
        var before = new ServiceCollection().EnableTwinboxDispatcher().AddTwinbox(b => b.SendOnly());
        var after = new ServiceCollection().AddTwinbox(b => b.SendOnly()).EnableTwinboxDispatcher();

        Assert.True(DispatcherEnabled(before));
        Assert.True(DispatcherEnabled(after));
    }

    [Fact]
    public void SendOnly_TurnsTheDispatcherOff()
    {
        Assert.False(DispatcherEnabled(new ServiceCollection().AddTwinbox(b => b.SendOnly())));
    }

    [Fact]
    public async Task Topology_ReportsRoutesHandlersAndKnownTypes()
    {
        await using var host = TestHost.Create(b => b
            .Route<DomainEvent>().To("domain-events")
            .AddHandler<TrialFeedbackTests.EventRelay<DomainEvent>, DomainEvent>("relay"));
        var topology = host.Services.GetRequiredService<ITwinboxTopology>();

        Assert.Equal(["domain-events"], topology.DestinationsOf(typeof(CustomerRegistered)));
        Assert.Equal(["relay"], topology.ConsumersOf(typeof(CustomerRegistered)));
        Assert.False(topology.IsKnown(typeof(CustomerRegistered)));
        Assert.Empty(topology.DestinationsOf(typeof(OrderPlaced)));
    }

    [Fact]
    public async Task TopologyChecks_FindSubtypesABaseRegistrationMisses()
    {
        await using var partial = TestHost.Create(b => b
            .Route<DomainEvent>().To("domain-events")
            .AddHandler<TrialFeedbackTests.EventRelay<DomainEvent>, DomainEvent>());
        await using var complete = TestHost.Create(b => b
            .Route<DomainEvent>().To("domain-events")
            .AddHandler<TrialFeedbackTests.EventRelay<DomainEvent>, DomainEvent>()
            .AddSubtypesOf<DomainEvent>());

        Assert.Empty(partial.Services.FindUnroutedSubtypes<DomainEvent>());
        Assert.Equal([typeof(CustomerRegistered)], partial.Services.FindUnhandledSubtypes<DomainEvent>());
        Assert.Empty(complete.Services.FindUnhandledSubtypes<DomainEvent>());
    }

    [Fact]
    public void RecordingOutbox_RecordsWhatWasSent()
    {
        var outbox = new RecordingOutbox();
        var options = new SendOptions { PartitionKey = "c-1" };

        outbox.Send(new CustomerRegistered(1), options);
        outbox.Send(new OrderPlaced(2));

        Assert.Equal([new CustomerRegistered(1)], outbox.Messages<CustomerRegistered>());
        Assert.Same(options, outbox.Sent[0].Options);
        Assert.Equal(2, outbox.Sent.Count);
    }

    private static bool DispatcherEnabled(IServiceCollection services)
    {
        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<IOptions<TwinboxOptions>>().Value.Dispatcher.Enabled;
    }
}
