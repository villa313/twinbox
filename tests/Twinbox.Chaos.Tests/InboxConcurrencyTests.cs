using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Transport;

namespace Twinbox.Chaos.Tests;

[Trait("Category", "Chaos")]
public abstract class InboxConcurrencyTests(ChaosDatabase database)
{
    private const int Instances = 3;
    private const int MaxDeliveries = 10;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    [Fact]
    public async Task ConcurrentDuplicates_RunEachConsumerOnce_EvenWhenItFailsFirst()
    {
        const int messages = 40;
        const int copies = 6;
        var schema = ChaosDatabase.NewSchema();
        var runs = new RunCounter();
        await using var hosts = await StartAsync(
            schema,
            b => b.AddHandler<SteadyConsumer, Numbered>(SteadyConsumer.Name).AddHandler<FailFirstConsumer, Numbered>(FailFirstConsumer.Name),
            runs);
        var failures = new ConcurrentQueue<Exception>();

        // Copies of a message are started together and spread over the instances, like a broker redelivering to several nodes.
        using var inFlight = new SemaphoreSlim(8 * copies);
        var deliveries = Enumerable.Range(0, messages)
            .SelectMany(id => Enumerable.Range(0, copies).Select(copy => (Message: Incoming(id), Instance: hosts[(id + copy) % Instances])))
            .Select(async delivery =>
            {
                await inFlight.WaitAsync();
                try
                {
                    var pipeline = delivery.Instance.Services.GetRequiredService<IInboundPipeline>();
                    await RedeliverUntilDoneAsync(() => pipeline.ProcessAsync(delivery.Message, CancellationToken.None), failures);
                }
                finally
                {
                    inFlight.Release();
                }
            });
        await Task.WhenAll(deliveries);

        var ids = Enumerable.Range(0, messages).Select(MessageId).ToArray();
        string[] consumers = [SteadyConsumer.Name, FailFirstConsumer.Name];
        var expected = ids.SelectMany(id => consumers.Select(consumer => (id, consumer))).Order().ToArray();
        Assert.Equal(expected, (await database.EffectsAsync(schema)).Order());
        Assert.Equal(expected.Length, await database.CountAsync(schema, "TwinboxInbox"));
        Assert.Equal(
            expected,
            (await database.OutboxAsync(schema)).Select(r => JsonSerializer.Deserialize<Handled>(r.Payload, Json)!).Select(h => (h.MessageId, h.Consumer)).Order());

        // Duplicates wait for the first run and then skip, so no consumer runs again after committing.
        Assert.All(ids, id => Assert.Equal(1, runs.Runs(id, SteadyConsumer.Name)));
        Assert.All(ids, id => Assert.Equal(2, runs.Runs(id, FailFirstConsumer.Name)));
        Assert.All(failures, f => Assert.IsType<FirstRunException>(f));
    }

    [Fact]
    public async Task OverlappingBatches_RunEachMessageOnce()
    {
        const int messages = 60;
        const int workers = 6;
        const int batchSize = 12;
        var schema = ChaosDatabase.NewSchema();
        var runs = new RunCounter();
        await using var hosts = await StartAsync(schema, b => b.AddBatchHandler<BatchConsumer, Numbered>(BatchConsumer.Name), runs);
        var failures = new ConcurrentQueue<Exception>();

        // Every worker delivers every message once, grouped and ordered differently, all at the same time.
        var random = new Random(41);
        var work = Enumerable.Range(0, workers).Select(worker =>
        {
            var order = Enumerable.Range(0, messages).ToArray();
            random.Shuffle(order);
            var pipeline = hosts[worker % Instances].Services.GetRequiredService<IInboundPipeline>();
            return Task.Run(async () =>
            {
                foreach (var batch in order.Chunk(batchSize))
                {
                    IncomingMessage[] items = [.. batch.Select(Incoming)];
                    await RedeliverUntilDoneAsync(() => pipeline.ProcessBatchAsync(items, CancellationToken.None), failures);
                }
            });
        });
        await Task.WhenAll(work);

        var ids = Enumerable.Range(0, messages).Select(MessageId).ToArray();
        Assert.Equal(ids.Select(id => (id, BatchConsumer.Name)).Order(), (await database.EffectsAsync(schema)).Order());
        Assert.Equal(messages, await database.CountAsync(schema, "TwinboxInbox"));
        Assert.Empty(failures);
    }

    [Fact]
    public async Task DistinctMessages_AreHandledInParallel()
    {
        const int parallel = 4;
        var schema = ChaosDatabase.NewSchema();
        await using var hosts = await StartAsync(schema, b => b.AddHandler<RendezvousConsumer, Numbered>(RendezvousConsumer.Name), new Rendezvous(parallel));
        var pipeline = hosts[0].Services.GetRequiredService<IInboundPipeline>();

        // Each handler waits inside its inbox transaction for the others, so an inbox that serializes new messages times out.
        await Task.WhenAll(Enumerable.Range(0, parallel).Select(id => Task.Run(() => pipeline.ProcessAsync(Incoming(id), CancellationToken.None))));

        Assert.Equal(parallel, (await database.EffectsAsync(schema)).Count);
    }

    private static string MessageId(int id) => $"message-{id}";

    private static IncomingMessage Incoming(int id) => new(
        MessageId(id),
        "numbered",
        Workload.Destination,
        JsonSerializer.SerializeToUtf8Bytes(new Numbered(null, id, id), Json),
        "application/json",
        new Dictionary<string, string>(),
        1,
        null);

    /// <summary>Retries like a broker would, keeping every failure so the test can tell expected ones from the rest.</summary>
    private static async Task RedeliverUntilDoneAsync(Func<Task> deliver, ConcurrentQueue<Exception> failures)
    {
        for (var delivery = 1; ; delivery++)
        {
            try
            {
                await deliver();
                return;
            }
            catch (Exception ex) when (delivery < MaxDeliveries)
            {
                failures.Enqueue(ex);
                await Task.Delay(TimeSpan.FromMilliseconds(10 * delivery));
            }
        }
    }

    private async Task<HostGroup> StartAsync(string schema, Action<TwinboxBuilder> handlers, params object[] singletons)
    {
        var hosts = new HostGroup();
        var log = new DeliveryLog();
        for (var i = 0; i < Instances; i++)
        {
            var instanceId = $"consumer-{i}";
            await hosts.AddAsync(ChaosHost.StartAsync(
                instanceId,
                b =>
                {
                    database.UseStore(b, schema);
                    b.Services.AddSingleton<ITransport>(new ChaosTransport(instanceId, log));
                    b.Route<Handled>().To("follow-ups").Configure(o => Workload.Dispatch(o, enabled: false));
                    handlers(b);
                },
                services =>
                {
                    services.AddSingleton(new EffectStatement(database.InsertEffect(schema)));
                    foreach (var singleton in singletons)
                    {
                        services.AddSingleton(singleton.GetType(), singleton);
                    }
                }));
        }

        await database.PrepareEffectsAsync(schema);
        return hosts;
    }
}

public sealed class PostgreSqlInboxConcurrencyTests(PostgreSqlDatabase database) : InboxConcurrencyTests(database);

public sealed class SqlServerInboxConcurrencyTests(SqlServerDatabase database) : InboxConcurrencyTests(database);
