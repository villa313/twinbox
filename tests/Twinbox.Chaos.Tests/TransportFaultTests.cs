using System.Diagnostics;
using Twinbox.Storage;

namespace Twinbox.Chaos.Tests;

[Trait("Category", "Chaos")]
public sealed class TransportFaultTests(PostgreSqlDatabase database)
{
    private const int Dispatchers = 3;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(90);

    [Fact]
    public async Task FlakyTransport_LeavesEveryMessageSentOrDead_WithEverySendCounted()
    {
        const int maxAttempts = 5;
        var plan = new FaultPlan(seed: 31);
        var schema = ChaosDatabase.NewSchema();
        var log = new DeliveryLog();
        await using var hosts = new HostGroup();
        for (var i = 0; i < Dispatchers; i++)
        {
            await hosts.AddAsync(Workload.StartAsync(
                database,
                schema,
                $"dispatcher-{i}",
                log,
                o =>
                {
                    Workload.Dispatch(o, batchSize: 20);
                    o.Dispatcher.SendTimeout = TimeSpan.FromMilliseconds(200);
                    o.Retry.MaxAttempts = maxAttempts;
                    o.Retry.InitialDelay = TimeSpan.FromMilliseconds(20);
                    o.Retry.MaxDelay = TimeSpan.FromMilliseconds(200);
                    o.CircuitBreaker.FailureThreshold = 4;
                    o.CircuitBreaker.BreakDuration = TimeSpan.FromMilliseconds(250);
                },
                plan.InjectAsync));
        }

        var messages = Workload.Mixed(seed: 32, keyed: 150, unkeyed: 150, streams: 10);
        await Workload.SendAsync(hosts[0], database, messages);
        var rows = await WaitUntilSettledAsync(schema, messages.Count);

        var deliveredIds = log.Deliveries.Select(d => d.MessageId).ToHashSet();
        foreach (var row in rows)
        {
            var (status, attempts) = plan.Expected(row.Message.Id, maxAttempts);
            Assert.True(
                (status, attempts) == ((OutboxMessageStatus)row.Status, row.Attempts),
                $"Message {row.Message.Id} ended {(OutboxMessageStatus)row.Status} after {row.Attempts} attempt(s); expected {status} after {attempts}.");
            Assert.Equal(attempts, log.CallsFor(row.Id));
            Assert.Equal(status == OutboxMessageStatus.Sent, deliveredIds.Contains(row.Id));
        }

        Assert.Contains(rows, r => r.Status == (int)OutboxMessageStatus.Dead);
        Assert.All(rows.Where(r => r.Status == (int)OutboxMessageStatus.Dead), r => Assert.NotNull(r.LastError));
        Assert.Equal(log.Deliveries.Count, deliveredIds.Count);
        log.AssertStreamOrder();
    }

    [Fact]
    public async Task BrokerOutage_CircuitBreakerHoldsMessagesBack_WithoutLosingThemOrBurningAttempts()
    {
        const int threshold = 3;
        var breakDuration = TimeSpan.FromMilliseconds(500);
        var outage = new Outage { Active = true };
        var schema = ChaosDatabase.NewSchema();
        var log = new DeliveryLog();
        await using var hosts = new HostGroup();
        for (var i = 0; i < 2; i++)
        {
            await hosts.AddAsync(Workload.StartAsync(
                database,
                schema,
                $"dispatcher-{i}",
                log,
                o =>
                {
                    Workload.Dispatch(o);
                    o.Retry.MaxAttempts = 20;
                    o.Retry.InitialDelay = TimeSpan.FromMilliseconds(50);
                    o.Retry.MaxDelay = TimeSpan.FromMilliseconds(500);
                    o.CircuitBreaker.FailureThreshold = threshold;
                    o.CircuitBreaker.BreakDuration = breakDuration;
                },
                outage.InjectAsync));
        }

        var outageClock = Stopwatch.StartNew();
        var messages = Workload.Mixed(seed: 33, keyed: 100, unkeyed: 100, streams: 5);
        await Workload.SendAsync(hosts[0], database, messages);
        await Task.Delay(TimeSpan.FromSeconds(3));
        outage.Active = false;
        outageClock.Stop();
        var rows = await WaitUntilSettledAsync(schema, messages.Count);

        Assert.All(rows, r => Assert.Equal((int)OutboxMessageStatus.Sent, r.Status));
        Assert.All(rows, r => Assert.Equal(log.CallsFor(r.Id), r.Attempts));
        Assert.Equal(messages.Count, log.DistinctDelivered);

        // Per instance: the failures that open the circuit, then one half-open probe per break.
        var failedSends = log.TotalCalls - log.Deliveries.Count;
        var probes = (int)Math.Ceiling(outageClock.Elapsed / breakDuration) + 1;
        Assert.InRange(failedSends, threshold, hosts.Hosts.Count * (threshold + probes));
        log.AssertStreamOrder();
    }

    private async Task<IReadOnlyList<OutboxRow>> WaitUntilSettledAsync(string schema, int count)
    {
        IReadOnlyList<OutboxRow> rows = [];
        await Eventually.HoldsAsync(
            async () =>
            {
                rows = await database.OutboxAsync(schema);
                return rows.Count == count && rows.All(r => r.Status is (int)OutboxMessageStatus.Sent or (int)OutboxMessageStatus.Dead);
            },
            Timeout,
            () => $"{rows.Count(r => r.Status is (int)OutboxMessageStatus.Pending or (int)OutboxMessageStatus.Processing)} message(s) never settled");
        return rows;
    }
}
