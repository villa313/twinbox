using StackExchange.Redis;
using Testcontainers.Redis;

namespace Twinbox.RedisStreams.Tests;

public sealed class RedisFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder("redis:7-alpine").Build();
    private ConnectionMultiplexer? _multiplexer;

    public string Configuration => _container.GetConnectionString();

    /// <summary>A separate client for arranging and inspecting streams behind the transport's back.</summary>
    public IDatabase Database => _multiplexer!.GetDatabase();

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _multiplexer = await ConnectionMultiplexer.ConnectAsync(Configuration);
    }

    public async ValueTask DisposeAsync()
    {
        if (_multiplexer is not null)
        {
            await _multiplexer.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    public async Task<long> PendingCountAsync(string stream, string group) =>
        (await Database.StreamPendingAsync(stream, group)).PendingMessageCount;

    public async Task<StreamEntry[]> WaitForEntriesAsync(string stream, int count, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            var entries = await Database.StreamRangeAsync(stream);
            if (entries.Length >= count)
            {
                return entries;
            }

            await Task.Delay(50, cts.Token);
        }
    }

    public async Task WaitForNoPendingAsync(string stream, string group, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (await PendingCountAsync(stream, group) != 0)
        {
            await Task.Delay(50, cts.Token);
        }
    }
}
