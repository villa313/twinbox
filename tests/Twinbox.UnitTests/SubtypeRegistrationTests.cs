using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Messaging;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class SubtypeRegistrationTests
{
    [Fact]
    public async Task BaseTypeHandler_WithoutSubtypes_CannotResolveASubtypeItHasNotSent()
    {
        await using var host = TestHost.Create(b => b
            .Route<DomainEvent>().To("domain-events")
            .AddHandler<TrialFeedbackTests.EventRelay<DomainEvent>, DomainEvent>(),
            s => s.AddSingleton(new InboxTests.Journal()));

        await Assert.ThrowsAnyAsync<Exception>(() => ProcessCustomerRegisteredAsync(host));
    }

    [Fact]
    public async Task AddSubtypesOf_ResolvesSubtypesAfterARestart()
    {
        var journal = new InboxTests.Journal();
        await using var host = TestHost.Create(b => b
            .Route<DomainEvent>().To("domain-events")
            .AddHandler<TrialFeedbackTests.EventRelay<DomainEvent>, DomainEvent>()
            .AddSubtypesOf<DomainEvent>(),
            s => s.AddSingleton(journal));

        await ProcessCustomerRegisteredAsync(host);

        Assert.Equal(["CustomerRegistered { CustomerId = 3 }"], journal.Entries);
    }

    [Fact]
    public void AddSubtypesOf_SkipsAbstractTypesAndTheBaseItself()
    {
        var builder = new TwinboxBuilder(new ServiceCollection(), new RouteTable(), new MessageTypeRegistry());

        builder.AddSubtypesOf<DomainEvent>();

        Assert.True(builder.MessageTypes.TryResolve("CustomerRegistered", out _));
        Assert.False(builder.MessageTypes.TryResolve("DomainEvent", out _));
    }

    // A fresh process receiving a message another instance (or its own previous run) sent.
    private static Task ProcessCustomerRegisteredAsync(TestHost host) =>
        host.Services.GetRequiredService<IInboundPipeline>().ProcessAsync(
            new IncomingMessage(Guid.NewGuid().ToString(), "CustomerRegistered", "domain-events",
                Encoding.UTF8.GetBytes("""{"customerId":3}"""), "application/json", new Dictionary<string, string>(), 1, null),
            default);
}
