using Twinbox.Transport;

namespace Twinbox.Benchmarks;

public sealed record OrderPlaced(Guid OrderId, string Customer, decimal Total);

public sealed record OrderSubmitted(Guid OrderId, string Customer, string ShippingAddress, IReadOnlyList<OrderLine> Lines);

public sealed record OrderLine(string Sku, string Description, int Quantity, decimal UnitPrice);

public sealed class NoOpHandler : IHandle<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, MessageContext context, CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>Accepts every message instantly, so dispatcher runs measure the store and not a broker.</summary>
public sealed class NoOpTransport : ITransport
{
    public const string TransportName = "noop";

    public string Name => TransportName;

    public Task SendAsync(TransportMessage message, CancellationToken cancellationToken) => Task.CompletedTask;
}

public static class SampleMessages
{
    public static OrderPlaced Small() => new(Guid.NewGuid(), "customer-42", 129.95m);

    /// <summary>About 2 KB of JSON: twenty order lines.</summary>
    public static OrderSubmitted Medium() => new(
        Guid.NewGuid(),
        "customer-42",
        "1 Infinite Loop, Cupertino, CA 95014, United States",
        [.. Enumerable.Range(1, 20).Select(i => new OrderLine($"SKU-{i:D5}", $"Line item number {i} with a description", i, 9.99m * i))]);
}
