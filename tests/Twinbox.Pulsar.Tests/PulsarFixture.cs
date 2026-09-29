using System.Buffers;
using System.Text.Json;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Testcontainers.Pulsar;

namespace Twinbox.Pulsar.Tests;

public sealed class PulsarFixture : IAsyncLifetime
{
    // Standalone Pulsar sizes its JVM for a whole machine unless told otherwise.
    private readonly PulsarContainer _container = new PulsarBuilder("apachepulsar/pulsar:4.0.13")
        .WithEnvironment("PULSAR_MEM", "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=256m")
        .Build();

    private readonly HttpClient _http = new();
    private IPulsarClient? _client;

    public string ServiceUrl => _container.GetBrokerAddress();

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _client = PulsarClient.Builder().ServiceUrl(new Uri(ServiceUrl)).Build();
    }

    public async ValueTask DisposeAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync();
        }

        _http.Dispose();
        await _container.DisposeAsync();
    }

    /// <summary>Reads a topic from the start with a separate client, behind the transport's back.</summary>
    public async Task<IReadOnlyList<IMessage<ReadOnlySequence<byte>>>> ReadAsync(string topic, int count, TimeSpan timeout)
    {
        await using var reader = _client!.NewReader().Topic(topic).StartMessageId(MessageId.Earliest).Create();
        using var cts = new CancellationTokenSource(timeout);
        var messages = new List<IMessage<ReadOnlySequence<byte>>>();
        while (messages.Count < count)
        {
            messages.Add(await reader.Receive(cts.Token));
        }

        return messages;
    }

    /// <summary>Messages the subscription has not acknowledged, delivered or not.</summary>
    public async Task<long> BacklogAsync(string topic, string subscription)
    {
        var stats = await _http.GetStringAsync(new Uri(new Uri(_container.GetServiceAddress()), $"admin/v2/persistent/public/default/{topic}/stats"));
        using var document = JsonDocument.Parse(stats);
        return document.RootElement.GetProperty("subscriptions").GetProperty(subscription).GetProperty("msgBacklog").GetInt64();
    }

    public async Task WaitForEmptyBacklogAsync(string topic, string subscription, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (await BacklogAsync(topic, subscription) != 0)
        {
            await Task.Delay(100, cts.Token);
        }
    }
}
