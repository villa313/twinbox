using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Twinbox.RabbitMQ;

/// <summary>The one long-lived connection shared by publishing and consuming; opened on first use.</summary>
internal sealed partial class RabbitMQConnection(IOptions<RabbitMQOptions> options, ILogger<RabbitMQConnection> logger) : IAsyncDisposable
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger = logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;
    private bool _disposed;

    /// <summary>Opens the connection on first call; later calls reuse it while automatic recovery keeps it alive.</summary>
    public async Task<IConnection> GetAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _connection) is { } existing)
        {
            return existing;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_connection is null)
            {
                var settings = options.Value;
                var connection = await CreateFactory(settings).CreateConnectionAsync(settings.ClientProvidedName, cancellationToken).ConfigureAwait(false);
                connection.ConnectionShutdownAsync += OnShutdownAsync;
                connection.RecoverySucceededAsync += OnRecoveredAsync;
                LogConnected(connection.Endpoint.HostName, connection.Endpoint.Port);
                Volatile.Write(ref _connection, connection);
            }

            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Keeps retrying the first connection so an unavailable broker delays consumers instead of failing startup.</summary>
    public async Task<IConnection> WaitForConnectionAsync(CancellationToken cancellationToken)
    {
        var delay = InitialRetryDelay;
        while (true)
        {
            try
            {
                return await GetAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not ObjectDisposedException && !cancellationToken.IsCancellationRequested)
            {
                LogConnectFailed(ex, delay);
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks));
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
            if (_connection is { } connection)
            {
                try
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogCloseFailed(ex);
                }

                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    internal static ConnectionFactory CreateFactory(RabbitMQOptions settings)
    {
        var factory = new ConnectionFactory
        {
            HostName = settings.HostName,
            VirtualHost = settings.VirtualHost,
            UserName = settings.UserName,
            Password = settings.Password,
            ClientProvidedName = settings.ClientProvidedName,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
        };

        if (settings.Port is { } port)
        {
            factory.Port = port;
        }

        if (settings.ConnectionUri is { } uri)
        {
            factory.Uri = uri;
        }

        settings.ConfigureConnectionFactory?.Invoke(factory);
        return factory;
    }

    private Task OnShutdownAsync(object sender, ShutdownEventArgs args)
    {
        if (args.Initiator != ShutdownInitiator.Application)
        {
            LogConnectionLost(args.ReplyCode, args.ReplyText);
        }

        return Task.CompletedTask;
    }

    private Task OnRecoveredAsync(object sender, AsyncEventArgs args)
    {
        LogRecovered();
        return Task.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Connected to RabbitMQ at {Host}:{Port}.")]
    private partial void LogConnected(string host, int port);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not connect to RabbitMQ; retrying in {Delay}.")]
    private partial void LogConnectFailed(Exception error, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "RabbitMQ connection lost ({ReplyCode} {ReplyText}); recovering automatically.")]
    private partial void LogConnectionLost(ushort replyCode, string replyText);

    [LoggerMessage(Level = LogLevel.Information, Message = "RabbitMQ connection recovered.")]
    private partial void LogRecovered();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Closing the RabbitMQ connection failed.")]
    private partial void LogCloseFailed(Exception error);
}
