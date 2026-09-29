using Microsoft.Extensions.DependencyInjection;
using Twinbox.Transport;

namespace Twinbox.UnitTests;

public sealed class MessagingFeatureTests
{
    [Fact]
    public async Task Filters_WrapHandlersInRegistrationOrder()
    {
        var journal = new InboxTests.Journal();
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders")
                .AddHandler<InboxTests.OrderPlacedHandler, OrderPlaced>()
                .AddFilter<OuterFilter>()
                .AddFilter<InnerFilter>(),
            s => s.AddSingleton(journal));

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        Assert.Equal(["outer:before", "inner:before", "OrderPlacedHandler:1", "inner:after", "outer:after"], journal.Entries);
    }

    [Fact]
    public async Task OutgoingFilters_CanStampHeaders()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders").AddOutgoingFilter<StampingFilter>());

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        Assert.Equal("web", Assert.Single(host.Harness.Transport.Sent).Headers["x-origin"]);
    }

    [Fact]
    public async Task MessagesSentFromAHandler_ShareTheConversationsCorrelationId()
    {
        await using var host = TestHost.Create(
            b => b.Route<OrderPlaced>().To("orders").Route<OrderShipped>().To("shipping").AddHandler<InboxTests.ShipOrderHandler, OrderPlaced>(),
            s => s.AddSingleton(new InboxTests.Journal()));

        await host.SendAsync(o => o.Send(new OrderPlaced(1), new SendOptions { CorrelationId = "checkout-42" }));
        await host.Harness.DrainAsync();

        Assert.All(host.Harness.Transport.Sent, m => Assert.Equal("checkout-42", m.Headers[TransportHeaders.CorrelationId]));
        Assert.Equal(2, host.Harness.Transport.Sent.Count);
    }

    [Fact]
    public async Task Reply_GoesToTheRequestsReplyAddressWithItsCorrelation()
    {
        await using var host = TestHost.Create(b => b.Route<PriceRequest>().To("pricing").AddHandler<PricingHandler, PriceRequest>());

        await host.SendAsync(o => o.Send(new PriceRequest("sku-1"), new SendOptions { ReplyTo = "checkout-replies" }));
        await host.Harness.DrainAsync();

        var request = host.Harness.Transport.Sent.Single(m => m.Destination == "pricing");
        var reply = host.Harness.Transport.Sent.Single(m => m.Destination == "checkout-replies");
        Assert.Equal(request.MessageId, reply.Headers[TransportHeaders.CorrelationId]);
        Assert.Equal(new PriceQuote("sku-1", 9.99m), Assert.Single(host.Harness.Sent<PriceQuote>()));
    }

    [Fact]
    public async Task CloudEventsProfile_WritesBinaryModeHeaders()
    {
        await using var host = TestHost.Create(b => b.UseHeaderProfile(HeaderProfile.CloudEvents("/shop")).Route<OrderPlaced>().To("orders"));

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        var headers = Assert.Single(host.Harness.Transport.Sent).Headers;
        Assert.Equal("1.0", headers["ce-specversion"]);
        Assert.Equal("/shop", headers["ce-source"]);
        Assert.Equal("OrderPlaced", headers["ce-type"]);
        Assert.True(DateTimeOffset.TryParse(headers["ce-time"], System.Globalization.CultureInfo.InvariantCulture, out _));
    }

    [Fact]
    public async Task DestinationPrefix_IsAppliedToRoutes()
    {
        await using var host = TestHost.Create(b => b.Route<OrderPlaced>().To("orders").Configure(o => o.DestinationPrefix = "staging-"));

        await host.SendAsync(o => o.Send(new OrderPlaced(1)));
        await host.Harness.DrainAsync();

        Assert.Equal("staging-orders", Assert.Single(host.Harness.Transport.Sent).Destination);
    }

    public sealed record PriceRequest(string Sku);

    public sealed record PriceQuote(string Sku, decimal Price);

    public sealed class PricingHandler(IOutbox outbox) : IHandle<PriceRequest>
    {
        public Task HandleAsync(PriceRequest message, MessageContext context, CancellationToken cancellationToken)
        {
            outbox.Reply(context, new PriceQuote(message.Sku, 9.99m));
            return Task.CompletedTask;
        }
    }

    public sealed class OuterFilter(InboxTests.Journal journal) : IMessageFilter
    {
        public async Task InvokeAsync(object message, MessageContext context, Func<Task> continuation, CancellationToken cancellationToken)
        {
            journal.Add("outer:before");
            await continuation();
            journal.Add("outer:after");
        }
    }

    public sealed class InnerFilter(InboxTests.Journal journal) : IMessageFilter
    {
        public async Task InvokeAsync(object message, MessageContext context, Func<Task> continuation, CancellationToken cancellationToken)
        {
            journal.Add("inner:before");
            await continuation();
            journal.Add("inner:after");
        }
    }

    public sealed class StampingFilter : IOutgoingMessageFilter
    {
        public void OnSending(object message, IDictionary<string, string> headers) => headers["x-origin"] = "web";
    }
}
