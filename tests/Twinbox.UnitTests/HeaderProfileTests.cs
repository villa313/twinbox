using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class HeaderProfileTests
{
    private static readonly HeaderProfile Legacy = HeaderProfile.Prefixed("legacy");

    [Fact]
    public async Task OutgoingMessages_CarryTheProfilesHeaders()
    {
        await using var host = TestHost.Create(b => b.UseHeaderProfile(Legacy).Route<OrderPlaced>().To("orders"));

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        var sent = Assert.Single(host.Harness.Transport.Sent);
        Assert.Equal(sent.MessageId, sent.Headers["legacy-msg-id"]);
        Assert.Equal("OrderPlaced", sent.Headers["legacy-msg-name"]);
    }

    [Fact]
    public async Task IncomingMessages_AreIdentifiedByTheProfilesHeaders()
    {
        var journal = new InboxTests.Journal();
        await using var host = TestHost.Create(
            b => b.UseHeaderProfile(Legacy).AddHandler<InboxTests.OrderPlacedHandler, OrderPlaced>(),
            s => s.AddSingleton(journal));
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();
        var headers = new Dictionary<string, string> { ["legacy-msg-id"] = "7195478239", ["legacy-msg-name"] = "OrderPlaced" };
        var fromLegacySystem = new IncomingMessage("orders:0:42", string.Empty, "orders", Encoding.UTF8.GetBytes("""{"OrderId":3}"""), "application/json", headers, 1, null);

        await pipeline.ProcessAsync(fromLegacySystem, default);
        await pipeline.ProcessAsync(fromLegacySystem with { MessageId = "orders:0:43" }, default);

        Assert.Equal(["OrderPlacedHandler:3"], journal.Entries);
    }

    [Fact]
    public async Task MessageWithoutAnyId_IsRejected()
    {
        await using var host = TestHost.Create(b => b.AddHandler<InboxTests.OrderPlacedHandler, OrderPlaced>(), s => s.AddSingleton(new InboxTests.Journal()));
        var pipeline = host.Services.GetRequiredService<IInboundPipeline>();

        await Assert.ThrowsAsync<PermanentDeliveryException>(() => pipeline.ProcessAsync(
            new IncomingMessage(string.Empty, "OrderPlaced", "orders", Encoding.UTF8.GetBytes("{}"), "application/json", new Dictionary<string, string>(), 1, null),
            default));
    }
}
