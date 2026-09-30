using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.Chaos.Tests;

[Trait("Category", "Chaos")]
public abstract class CrashRecoveryTests(ChaosDatabase database)
{
    private const int DeliveredBeforeCrash = 10;
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task DispatcherStoppedMidSend_RecordsWhatItSent_AndSurvivorsDeliverTheRestOnce()
    {
        var schema = ChaosDatabase.NewSchema();
        var log = new DeliveryLog();
        await using var hosts = new HostGroup();
        var producer = await hosts.AddAsync(Workload.StartAsync(database, schema, "producer", log, o => Workload.Dispatch(o, enabled: false)));
        var messages = Workload.Mixed(seed: 21, keyed: 150, unkeyed: 150, streams: 10);
        await Workload.SendAsync(producer, database, messages);

        var sends = 0;
        var stalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var victim = await hosts.AddAsync(Workload.StartAsync(
            database,
            schema,
            "victim",
            log,
            o => Workload.Dispatch(o, batchSize: 100, lease: Lease),
            async (_, cancellationToken) =>
            {
                if (Interlocked.Increment(ref sends) <= DeliveredBeforeCrash)
                {
                    return;
                }

                stalled.TrySetResult();
                await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
            }));
        await stalled.Task.WaitAsync(Timeout);
        await victim.StopAsync();

        // A graceful stop records the sends that finished and releases the rest instead of leaving them leased.
        var rows = await database.OutboxAsync(schema);
        Assert.DoesNotContain(rows, r => r.Status == (int)OutboxMessageStatus.Processing);
        var sentByVictim = log.Deliveries.Select(d => d.MessageId).ToHashSet();
        Assert.Equal(DeliveredBeforeCrash, sentByVictim.Count);
        Assert.Equal(DeliveredBeforeCrash, rows.Count(r => r.Status == (int)OutboxMessageStatus.Sent));

        for (var i = 0; i < 2; i++)
        {
            await hosts.AddAsync(Workload.StartAsync(database, schema, $"survivor-{i}", log, o => Workload.Dispatch(o, batchSize: 50, lease: Lease)));
        }

        await WaitUntilAllSentAsync(schema, messages.Count);

        Assert.Equal(messages.Count, log.DistinctDelivered);
        var survivorDeliveries = log.Deliveries.Where(d => d.Instance != "victim").ToArray();
        Assert.DoesNotContain(survivorDeliveries, d => sentByVictim.Contains(d.MessageId));
        log.AssertStreamOrder();
    }

    [Fact]
    public async Task ClaimAbandonedByAVanishedInstance_IsDeliveredOnceTheLeaseRunsOut()
    {
        var schema = ChaosDatabase.NewSchema();
        var log = new DeliveryLog();
        await using var hosts = new HostGroup();
        var producer = await hosts.AddAsync(Workload.StartAsync(database, schema, "producer", log, o => Workload.Dispatch(o, enabled: false)));
        var messages = Workload.Mixed(seed: 22, keyed: 40, unkeyed: 40, streams: 4);
        await Workload.SendAsync(producer, database, messages);

        // A process killed between claiming and sending leaves its rows leased and never completes them.
        var store = producer.Services.GetServices<IOutboxStore>().Single();
        var claimedAt = DateTimeOffset.UtcNow;
        var abandoned = await store.ClaimAsync(new OutboxClaim("vanished", 1000, claimedAt, Lease), CancellationToken.None);
        Assert.NotEmpty(abandoned);

        await hosts.AddAsync(Workload.StartAsync(database, schema, "survivor", log, o => Workload.Dispatch(o, lease: Lease)));
        await WaitUntilAllSentAsync(schema, messages.Count);

        Assert.Equal(messages.Count, log.DistinctDelivered);
        var abandonedIds = abandoned.Select(m => m.Id).ToHashSet();
        Assert.All(log.Deliveries.Where(d => abandonedIds.Contains(d.MessageId)), d => Assert.True(d.At > claimedAt + Lease));
        log.AssertStreamOrder();
    }

    private Task WaitUntilAllSentAsync(string schema, int count) =>
        Eventually.HoldsAsync(
            async () =>
            {
                var rows = await database.OutboxAsync(schema);
                return rows.Count == count && rows.All(r => r.Status == (int)OutboxMessageStatus.Sent);
            },
            Timeout,
            () => "messages were left unsent");
}

public sealed class PostgreSqlCrashRecoveryTests(PostgreSqlDatabase database) : CrashRecoveryTests(database);

public sealed class SqlServerCrashRecoveryTests(SqlServerDatabase database) : CrashRecoveryTests(database);
