using Twinbox.Storage;

namespace Twinbox.Chaos.Tests;

[Trait("Category", "Chaos")]
public abstract class CompetingDispatcherTests(ChaosDatabase database)
{
    private const int Dispatchers = 8;
    private const int PerProducer = 2500;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromMinutes(2);

    [Fact]
    public async Task ManyDispatchers_DeliverEveryMessageOnce_InStreamOrder()
    {
        var schema = ChaosDatabase.NewSchema();
        var log = new DeliveryLog();
        await using var hosts = new HostGroup();
        for (var i = 0; i < Dispatchers; i++)
        {
            await hosts.AddAsync(Workload.StartAsync(database, schema, $"dispatcher-{i}", log, o => Workload.Dispatch(o, batchSize: 20)));
        }

        // Each producer owns its streams, so a stream's order is the order one producer committed it in.
        var first = Workload.Mixed(seed: 11, keyed: 1700, unkeyed: 800, streams: 12, streamPrefix: "a");
        var second = Workload.Mixed(seed: 12, keyed: 1700, unkeyed: 800, streams: 12, streamPrefix: "b", firstId: PerProducer);
        await Task.WhenAll(
            Workload.SendAsync(hosts[0], database, first),
            Workload.SendAsync(hosts[1], database, second));

        const int total = 2 * PerProducer;
        await Eventually.HoldsAsync(() => log.DistinctDelivered == total, DrainTimeout, () => $"{log.DistinctDelivered} of {total} delivered");
        await Eventually.HoldsAsync(
            async () => (await database.OutboxAsync(schema)).All(r => r.Status == (int)OutboxMessageStatus.Sent),
            DrainTimeout,
            () => "not every message was marked sent");

        var rows = await database.OutboxAsync(schema);
        Assert.Equal(total, rows.Count);
        Assert.All(rows, r => Assert.Equal(1, r.Attempts));
        Assert.Equal(rows.Select(r => r.Id).Order(), log.Deliveries.Select(d => d.MessageId).Order());
        log.AssertStreamOrder();
        Assert.True(log.Deliveries.Select(d => d.Instance).Distinct().Count() > 1, "only one dispatcher did any work");
        Assert.Empty(hosts.Hosts.SelectMany(h => h.Errors));
    }
}

public sealed class PostgreSqlCompetingDispatcherTests(PostgreSqlDatabase database) : CompetingDispatcherTests(database);

public sealed class SqlServerCompetingDispatcherTests(SqlServerDatabase database) : CompetingDispatcherTests(database);
