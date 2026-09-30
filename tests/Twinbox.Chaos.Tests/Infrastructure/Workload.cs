using Microsoft.Extensions.DependencyInjection;
using Twinbox.Transport;

namespace Twinbox.Chaos.Tests;

internal static class Workload
{
    public const string Destination = "orders";

    /// <summary>Keyed messages spread over <paramref name="streams"/> streams, shuffled among unkeyed ones by a seeded random.</summary>
    public static IReadOnlyList<Numbered> Mixed(int seed, int keyed, int unkeyed, int streams, string streamPrefix = "s", int firstId = 0)
    {
        var random = new Random(seed);
        var nextIndex = new int[streams];
        var kinds = Enumerable.Repeat(true, keyed).Concat(Enumerable.Repeat(false, unkeyed)).ToArray();
        random.Shuffle(kinds);

        var messages = new List<Numbered>(kinds.Length);
        foreach (var isKeyed in kinds)
        {
            var id = firstId + messages.Count;
            if (isKeyed)
            {
                var stream = random.Next(streams);
                messages.Add(new Numbered($"{streamPrefix}{stream}", nextIndex[stream]++, id));
            }
            else
            {
                messages.Add(new Numbered(null, id, id));
            }
        }

        return messages;
    }

    /// <summary>An instance sending <see cref="Numbered"/> through a <see cref="ChaosTransport"/> that shares <paramref name="log"/>.</summary>
    public static Task<ChaosHost> StartAsync(
        ChaosDatabase database,
        string schema,
        string instanceId,
        DeliveryLog log,
        Action<TwinboxOptions> options,
        Func<TransportMessage, CancellationToken, Task>? fault = null) =>
        ChaosHost.StartAsync(instanceId, twinbox =>
        {
            database.UseStore(twinbox, schema);
            twinbox.Services.AddSingleton<ITransport>(new ChaosTransport(instanceId, log, fault));
            twinbox.Route<Numbered>().To(Destination).Configure(options);
        });

    /// <summary>Commits <paramref name="messages"/> in order, <paramref name="perTransaction"/> per database transaction.</summary>
    public static async Task SendAsync(ChaosHost host, ChaosDatabase database, IEnumerable<Numbered> messages, int perTransaction = 25)
    {
        foreach (var chunk in messages.Chunk(perTransaction))
        {
            await using var scope = host.Services.CreateAsyncScope();
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            await using var connection = database.Connect();
            await connection.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            foreach (var message in chunk)
            {
                outbox.Send(message, new SendOptions { PartitionKey = message.Stream });
            }

            await outbox.CommitAsync(transaction);
        }
    }

    /// <summary>Short polls, and a lease far longer than any send unless a test is about leases running out.</summary>
    public static void Dispatch(TwinboxOptions options, int batchSize = 50, TimeSpan? lease = null, bool enabled = true)
    {
        options.Dispatcher.Enabled = enabled;
        options.Dispatcher.BatchSize = batchSize;
        options.Dispatcher.LeaseDuration = lease ?? TimeSpan.FromMinutes(1);
        options.Dispatcher.MinPollInterval = TimeSpan.FromMilliseconds(20);
        options.Dispatcher.MaxPollInterval = TimeSpan.FromMilliseconds(250);
        options.Dispatcher.MaxDegreeOfParallelism = 4;
    }
}
