using Microsoft.Extensions.DependencyInjection;

using Twinbox.Generated;

namespace Twinbox.UnitTests;

public sealed class GeneratedRegistrationTests
{
    [Fact]
    public async Task GeneratedRegistration_DeliversToHandlerWithoutManualAddHandler()
    {
        var journal = new InboxTests.Journal();
        await using var host = TestHost.Create(
            b => b.Route<RegistrationPing>().To("pings").AddHandlersFromTwinboxUnitTests(),
            s => s.AddSingleton(journal));

        await host.SendAsync(o => o.Send(new RegistrationPing(7)));
        await host.Harness.DrainAsync();

        Assert.Equal(["ping:7"], journal.Entries);
    }

    public sealed record RegistrationPing(int Id);

    public sealed class RegistrationPingHandler(InboxTests.Journal journal) : IHandle<RegistrationPing>
    {
        public Task HandleAsync(RegistrationPing message, MessageContext context, CancellationToken cancellationToken)
        {
            journal.Add($"ping:{message.Id}");
            return Task.CompletedTask;
        }
    }
}
