using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Testcontainers.Nats;

namespace Twinbox.Nats.Tests;

public sealed class NatsFixture : IAsyncLifetime
{
    // The module starts the server with JetStream enabled.
    private readonly NatsContainer _container = new NatsBuilder("nats:2-alpine").Build();
    private NatsConnection? _connection;

    public string Url => _container.GetConnectionString();

    public NatsJSContext JetStream { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _connection = new NatsConnection(NatsOpts.Default with { Url = Url });
        JetStream = new NatsJSContext(_connection);
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    public async Task CreateStreamAsync(string name, params string[] subjects) =>
        await JetStream.CreateStreamAsync(new StreamConfig(name, subjects), TestContext.Current.CancellationToken);

    public async Task<long> StoredMessagesAsync(string stream)
    {
        var info = await JetStream.GetStreamAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        return info.Info.State.Messages;
    }

    public async Task<ConsumerInfo> ConsumerInfoAsync(string stream, string consumer)
    {
        var info = await JetStream.GetConsumerAsync(stream, consumer, TestContext.Current.CancellationToken);
        return info.Info;
    }

    /// <summary>Reads the first message of <paramref name="stream"/> without acknowledging anything.</summary>
    public async Task<NatsJSMsg<byte[]>> ReadFirstAsync(string stream, TimeSpan timeout)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(timeout);
        var consumer = await JetStream.CreateOrderedConsumerAsync(stream, cancellationToken: cts.Token);
        await foreach (var msg in consumer.ConsumeAsync(NatsRawSerializer<byte[]>.Default, cancellationToken: cts.Token))
        {
            return msg;
        }

        throw new InvalidOperationException($"Stream {stream} ended without messages.");
    }
}
