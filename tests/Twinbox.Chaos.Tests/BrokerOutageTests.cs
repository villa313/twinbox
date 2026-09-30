using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;

namespace Twinbox.Chaos.Tests;

[Trait("Category", "Chaos")]
public sealed class BrokerOutageTests(PostgreSqlDatabase database, RabbitMqBroker broker) : IClassFixture<RabbitMqBroker>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromMinutes(2);

    [Fact]
    public async Task BrokerRestart_BacklogDrainsAfterRecovery_AndTheConsumerReconnects()
    {
        var schema = ChaosDatabase.NewSchema();
        var name = $"chaos-{Guid.NewGuid():N}"[..14];
        var journal = new Journal();
        await using var hosts = new HostGroup();
        var app = await hosts.AddAsync(ChaosHost.StartAsync(
            "app",
            b =>
            {
                database.UseStore(b, schema);
                b.UseRabbitMq(o =>
                    {
                        o.ConnectionUri = broker.ConnectionUri;
                        o.PublishTimeout = TimeSpan.FromSeconds(2);
                        o.Listen(name, name);
                        o.ConfigureConnectionFactory = f => f.NetworkRecoveryInterval = TimeSpan.FromSeconds(1);
                    })
                    .Route<Numbered>().To(name)
                    .AddHandler<JournalConsumer, Numbered>("journal")
                    .Configure(o =>
                    {
                        Workload.Dispatch(o);
                        o.Dispatcher.SendTimeout = TimeSpan.FromSeconds(3);
                        o.Retry.MaxAttempts = 1000;
                        o.Retry.InitialDelay = TimeSpan.FromMilliseconds(200);
                        o.Retry.MaxDelay = TimeSpan.FromSeconds(1);
                        o.CircuitBreaker.BreakDuration = TimeSpan.FromSeconds(1);
                    });
            },
            services => services.AddSingleton(journal)));

        await Workload.SendAsync(app, database, Workload.Mixed(seed: 51, keyed: 30, unkeyed: 30, streams: 3));
        await WaitForHandledAsync(journal, 60);

        await broker.StopAsync();
        await Workload.SendAsync(app, database, Workload.Mixed(seed: 52, keyed: 60, unkeyed: 60, streams: 3, streamPrefix: "t", firstId: 60));
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Equal(60, journal.Distinct);
        Assert.Equal(120, (await database.OutboxAsync(schema)).Count(r => r.Status != (int)OutboxMessageStatus.Sent));

        await broker.StartAsync();
        await WaitForHandledAsync(journal, 180);

        // Fresh traffic after the backlog shows publishing and consuming both came back, not just a replay.
        await Workload.SendAsync(app, database, Workload.Mixed(seed: 53, keyed: 20, unkeyed: 20, streams: 3, streamPrefix: "u", firstId: 180));
        await WaitForHandledAsync(journal, 220);

        await Eventually.HoldsAsync(
            async () => (await database.OutboxAsync(schema)).All(r => r.Status == (int)OutboxMessageStatus.Sent),
            Timeout,
            () => "the outbox did not drain");
        Assert.All(journal.Handled, h => Assert.Equal(1, h.Value));
        Assert.False(app.Crashed);
    }

    private static Task WaitForHandledAsync(Journal journal, int count) =>
        Eventually.HoldsAsync(() => journal.Distinct >= count, Timeout, () => $"{journal.Distinct} of {count} messages handled");
}
