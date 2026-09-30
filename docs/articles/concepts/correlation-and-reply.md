# Correlation and request/reply

## Correlation ids

Every message can carry a correlation id in the `twinbox-correlation-id` header. Handlers read it from
`MessageContext.CorrelationId`.

When you don't set one, a message sent **while handling another** inherits the handled message's correlation id, or
its message id if it had none. A whole conversation (order placed, payment taken, shipment booked) therefore shares one
id without any code:

```csharp
public class TakePayment(IOutbox outbox) : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        outbox.Send(new PaymentTaken(message.OrderId));   // correlation id = context.CorrelationId ?? context.MessageId
        return Task.CompletedTask;
    }
}
```

Set it explicitly when a conversation starts outside a handler, for example from an HTTP request id:

```csharp
outbox.Send(new OrderPlaced(order.Id), new SendOptions { CorrelationId = httpContext.TraceIdentifier });
```

Correlation is separate from tracing: the `traceparent` header links producer and consumer spans in OpenTelemetry
regardless. See [Observability](../observability.md).

## Request/reply

The requester says where to reply with `SendOptions.ReplyTo`; the responder answers with `outbox.Reply`:

```csharp
// Requester
outbox.Send(new QuoteRequested(cartId), new SendOptions { ReplyTo = "checkout-replies" });

// Responder
public class QuotePrices(IOutbox outbox) : IHandle<QuoteRequested>
{
    public Task HandleAsync(QuoteRequested request, MessageContext context, CancellationToken ct)
    {
        outbox.Reply(context, new PriceQuote(request.CartId, 42m));
        return Task.CompletedTask;
    }
}

// Requester again, listening on "checkout-replies"
public class ApplyQuote : IHandle<PriceQuote>
{
    public Task HandleAsync(PriceQuote quote, MessageContext context, CancellationToken ct)
    {
        // context.CorrelationId is the request's correlation id (or its message id)
        return Task.CompletedTask;
    }
}
```

`Reply` sends to `context.ReplyTo` through the outbox, so the reply commits with the responder's inbox transaction like
any other message. It throws `InvalidOperationException` if the request has no reply address. Pass `transport:` when
several transports are registered:

```csharp
outbox.Reply(context, response, transport: "rabbitmq");
```

Reply addresses are physical: neither `SendOptions.ReplyTo` nor `Reply` adds the
[destination prefix](routing.md#destination-prefix), so set `ReplyTo` to the address your listener uses (for example
`options.ToPhysicalDestination("checkout-replies")` when that listener follows the prefix). The requester
needs a listener on that address and a handler for the response type. Replies are asynchronous messages; there is no
blocking "send and wait" API.
