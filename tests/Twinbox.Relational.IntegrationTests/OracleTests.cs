using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.Relational.IntegrationTests;

public sealed class OracleTests(OracleDatabase database) : RelationalTests<OracleDatabase>(database)
{
    /// <summary>Rows are inserted from PL/SQL, where a plain RAW bind stops at 32 KB.</summary>
    [Fact]
    public async Task LargePayloadAndHeaders_RoundTrip()
    {
        await using var services = await StartAsync();
        var store = services.GetServices<IOutboxStore>().Single();
        var message = new OutboxMessage
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

        await store.AppendAsync([message], default);
        var claimed = Assert.Single(await store.ClaimAsync(new OutboxClaim("owner", 10, DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1)), default));

        Assert.Equal(message.Payload, claimed.Payload);
        Assert.Equal(message.Headers["x-large"], claimed.Headers["x-large"]);
    }
}
