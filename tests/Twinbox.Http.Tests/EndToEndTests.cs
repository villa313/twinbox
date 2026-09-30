using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.InMemory;
using Twinbox.Storage;

namespace Twinbox.Http.Tests;

public sealed record OrderPlaced(int OrderId);

public sealed class EndToEndTests
{
    [Fact]
    public async Task TransientFailure_IsRetriedThenDelivered()
    {
        await using var host = CreateHost();
        host.Handler.Respond(HttpStatusCode.ServiceUnavailable).Respond(HttpStatusCode.OK);

        await SendAsync(host, new OrderPlaced(42));
        await host.Harness.DrainAsync(TestContext.Current.CancellationToken);

        var row = Assert.Single(host.Harness.Store.Snapshot());
        Assert.Equal(OutboxMessageStatus.Pending, row.Status);
        Assert.Contains("503", row.LastError, StringComparison.Ordinal);

        host.Time.Advance(TimeSpan.FromMinutes(10));
        await host.Harness.DrainAsync(TestContext.Current.CancellationToken);

        row = Assert.Single(host.Harness.Store.Snapshot());
        Assert.Equal(OutboxMessageStatus.Sent, row.Status);
        Assert.Equal(2, host.Handler.Requests.Count);
        Assert.All(host.Handler.Requests, r => Assert.Equal(row.Id.ToString(), r.Headers["Idempotency-Key"]));
        Assert.Equal(42, JsonDocument.Parse(host.Handler.Requests[1].Body!).RootElement.GetProperty("orderId").GetInt32());
        Assert.Equal("https://api.vendor.test/orders/42", host.Handler.Requests[1].Url.ToString());
    }

    [Fact]
    public async Task BadRequest_IsDeadLettered()
    {
        await using var host = CreateHost();
        host.Handler.Respond(HttpStatusCode.BadRequest, r => r.Content = new StringContent("""{"error":"sku unknown"}"""));

        await SendAsync(host, new OrderPlaced(1));
        await host.Harness.DrainAsync(TestContext.Current.CancellationToken);

        var dead = Assert.Single(host.Harness.DeadLettered());
        Assert.Equal(1, dead.Attempts);
        Assert.Contains("sku unknown", dead.LastError, StringComparison.Ordinal);
        Assert.Single(host.Handler.Requests);
    }

    [Fact]
    public async Task TooManyRequests_WaitsAtLeastRetryAfter()
    {
        await using var host = CreateHost();
        host.Handler.Respond(HttpStatusCode.TooManyRequests, r => r.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(120)));

        await SendAsync(host, new OrderPlaced(1));
        await host.Harness.DrainAsync(TestContext.Current.CancellationToken);

        var row = Assert.Single(host.Harness.Store.Snapshot());
        Assert.Equal(OutboxMessageStatus.Pending, row.Status);
        Assert.True(row.AvailableAt >= host.Time.GetUtcNow().AddSeconds(120), $"Rescheduled for {row.AvailableAt}.");

        host.Time.Advance(TimeSpan.FromSeconds(119));
        await host.Harness.DrainAsync(TestContext.Current.CancellationToken);
        Assert.Single(host.Handler.Requests);

        host.Time.Advance(TimeSpan.FromSeconds(1));
        await host.Harness.DrainAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, host.Handler.Requests.Count);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Harness.Store.Snapshot()).Status);
    }

    private static HttpTestHost CreateHost() => HttpTestHost.Create(
        o => o.AddEndpoint("valuelink", e => e.Url = new Uri("https://api.vendor.test/orders/{partitionKey}")),
        b => b.Route<OrderPlaced>().To("valuelink", transport: "http"));

    private static async Task SendAsync(HttpTestHost host, OrderPlaced message)
    {
        await using var scope = host.Services.CreateAsyncScope();
        scope.ServiceProvider.GetRequiredService<IOutbox>()
            .Send(message, new SendOptions { PartitionKey = message.OrderId.ToString(System.Globalization.CultureInfo.InvariantCulture) });
        await scope.ServiceProvider.GetRequiredService<InMemoryUnitOfWork>().CommitAsync();
    }
}
