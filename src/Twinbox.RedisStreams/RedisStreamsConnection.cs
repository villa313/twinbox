using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Twinbox.RedisStreams;

/// <summary>The one multiplexer shared by sending and consuming; connected on first use.</summary>
internal sealed partial class RedisStreamsConnection(
    IOptions<RedisStreamsOptions> options,
    IServiceProvider services,
    ILogger<RedisStreamsConnection> logger) : IAsyncDisposable
{
    private readonly ILogger _logger = logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnectionMultiplexer? _multiplexer;
    private bool _owned;
    private bool _disposed;

    public RedisStreamsOptions Options { get; } = options.Value;

    public async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _multiplexer) is { } existing)
        {
            return existing.GetDatabase();
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_multiplexer is null)
            {
                var (multiplexer, owned) = await ConnectAsync().ConfigureAwait(false);
                _owned = owned;
                Volatile.Write(ref _multiplexer, multiplexer);
            }

            return _multiplexer.GetDatabase();
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (_owned && _multiplexer is { } multiplexer)
            {
                await multiplexer.CloseAsync().ConfigureAwait(false);
                multiplexer.Dispose();
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Keeps reconnecting in the background instead of failing when the server is down at startup.</summary>
    internal static ConfigurationOptions ParseConfiguration(string configuration)
    {
        var parsed = ConfigurationOptions.Parse(configuration);
        if (!configuration.Contains("abortConnect", StringComparison.OrdinalIgnoreCase))
        {
            parsed.AbortOnConnectFail = false;
        }

        return parsed;
    }

    private async Task<(IConnectionMultiplexer Multiplexer, bool Owned)> ConnectAsync()
    {
        if (Options.ConnectionFactory is { } factory)
        {
            return (factory(services), false);
        }

        var multiplexer = await ConnectionMultiplexer.ConnectAsync(ParseConfiguration(Options.Configuration!)).ConfigureAwait(false);
        LogConnected(multiplexer.ClientName, multiplexer.IsConnected);
        return (multiplexer, true);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created Redis multiplexer {ClientName} (connected: {Connected}).")]
    private partial void LogConnected(string clientName, bool connected);
}
