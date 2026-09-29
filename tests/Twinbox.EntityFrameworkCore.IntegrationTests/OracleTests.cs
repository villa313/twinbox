using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.EntityFrameworkCore.IntegrationTests;

public sealed class OracleTests(OracleFixture database) : EntityFrameworkTests<OracleFixture>(database)
{
    /// <summary>Oracle's provider would otherwise map the payload to RAW(2000) and the headers to NVARCHAR2(2000).</summary>
    [Fact]
    public async Task LargePayloadAndHeaders_RoundTrip()
    {
        await using var services = BuildServices();
        var store = services.GetRequiredService<IOutboxStore>();
        var message = LargeMessage();

        await store.AppendAsync([message], default);
        var claimed = Assert.Single(await store.ClaimAsync(new OutboxClaim("owner", 10, DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1)), default));

        Assert.Equal(message.Payload, claimed.Payload);
        Assert.Equal(message.Headers["x-large"], claimed.Headers["x-large"]);
    }

    private static OutboxMessage LargeMessage() => new()
    {
        Id = Guid.NewGuid(),
        MessageName = "large",
        Transport = "test",
        Destination = "orders",
        Payload = [.. Enumerable.Range(0, 100_000).Select(i => (byte)i)],
        ContentType = "application/octet-stream",
        Headers = new Dictionary<string, string> { ["x-large"] = new string('h', 5_000) },
        CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
        AvailableAt = DateTimeOffset.UtcNow.AddMinutes(-1),
    };
}
