using System.Data.Common;
using Twinbox.Storage;

namespace Twinbox.Chaos.Tests;

[Trait("Category", "Chaos")]
public sealed class DatabaseBlipTests(RestartablePostgreSqlDatabase database) : IClassFixture<RestartablePostgreSqlDatabase>
{
    private const int Streams = 4;
    private const int PerRound = 20;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(90);

    [Fact]
    public async Task Dispatchers_SurviveKilledConnectionsAPauseAndARestart_AndDeliverEverythingCommitted()
    {
        var schema = ChaosDatabase.NewSchema();
        var log = new DeliveryLog();
        var lease = TimeSpan.FromSeconds(3);
        await using var hosts = new HostGroup();
        var producer = await hosts.AddAsync(Workload.StartAsync(database, schema, "producer", log, o => Workload.Dispatch(o, enabled: false)));
        for (var i = 0; i < 2; i++)
        {
            await hosts.AddAsync(Workload.StartAsync(database, schema, $"dispatcher-{i}", log, o => Workload.Dispatch(o, batchSize: 20, lease: lease)));
        }

        using var stop = new CancellationTokenSource();
        var producing = ProduceUntilStoppedAsync(producer, stop.Token);

        await Task.Delay(TimeSpan.FromSeconds(1));
        for (var i = 0; i < 3; i++)
        {
            await database.TerminateConnectionsAsync();
            await Task.Delay(TimeSpan.FromMilliseconds(700));
        }

        await database.PauseAsync(TimeSpan.FromSeconds(3));
        await Task.Delay(TimeSpan.FromSeconds(1));
        await database.RestartAsync();
        var committedBeforeRestart = await CountCommittedAsync(schema);
        await Task.Delay(TimeSpan.FromSeconds(2));
        await stop.CancelAsync();
        await producing;

        IReadOnlyList<OutboxRow> rows = [];
        await Eventually.HoldsAsync(
            async () =>
            {
                try
                {
                    rows = await database.OutboxAsync(schema);
                }
                catch (DbException)
                {
                    return false;
                }

                return rows.All(r => r.Status == (int)OutboxMessageStatus.Sent);
            },
            Timeout,
            () => $"{rows.Count(r => r.Status != (int)OutboxMessageStatus.Sent)} message(s) were never sent");

        Assert.True(rows.Count > committedBeforeRestart, "nothing was committed after the restart");
        Assert.Equal(rows.Select(r => r.Id).Order(), log.Deliveries.Select(d => d.MessageId).Distinct().Order());
        Assert.All(hosts.Hosts, h => Assert.False(h.Crashed, $"{h.InstanceId} stopped"));
        Assert.Contains(hosts.Hosts.Skip(1), h => h.Errors.Count > 0);
        log.AssertStreamOrder();
    }

    /// <summary>Keeps committing like a busy app; failed commits are dropped, so the outbox alone says what must be delivered.</summary>
    private async Task ProduceUntilStoppedAsync(ChaosHost producer, CancellationToken stop)
    {
        var nextIndex = new int[Streams];
        var nextId = 0;
        while (!stop.IsCancellationRequested)
        {
            var round = Enumerable.Range(0, PerRound)
                .Select(i => i % 2 == 0
                    ? new Numbered($"s{i / 2 % Streams}", nextIndex[i / 2 % Streams]++, nextId++)
                    : new Numbered(null, nextId, nextId++))
                .ToArray();
            try
            {
                await Workload.SendAsync(producer, database, round, perTransaction: PerRound);
            }
            catch (DbException)
            {
                // The database is down or the connection was cut; the app would report the failure and move on.
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        }
    }

    private async Task<int> CountCommittedAsync(string schema)
    {
        while (true)
        {
            try
            {
                return await database.CountAsync(schema, "TwinboxOutbox");
            }
            catch (DbException)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200));
            }
        }
    }
}
