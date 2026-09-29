using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Twinbox.Transport;

namespace Twinbox.RabbitMQ;

/// <summary>Publishes to a topic exchange named by the destination, using the message name as the routing key.</summary>
public sealed partial class RabbitMqTransport : ITransport, IAsyncDisposable
{
    public const string DefaultName = "rabbitmq";

    private readonly ILogger _logger;
    private readonly RabbitMqOptions _options;
    private readonly RabbitMqChannelPool _channels;
    private readonly ConcurrentDictionary<string, bool> _declaredExchanges = new(StringComparer.Ordinal);

    internal RabbitMqTransport(RabbitMqConnection connection, RabbitMqOptions options, ILogger<RabbitMqTransport> logger)
    {
        _logger = logger;
        _options = options;
        _channels = new RabbitMqChannelPool(connection, options.PublishChannelPoolSize);
    }

    public string Name => DefaultName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_options.PublishTimeout);

        IChannel channel;
        try
        {
            channel = await _channels.RentAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"No RabbitMQ channel became available within {_options.PublishTimeout}.", ex);
        }

        var discard = false;
        try
        {
            await EnsureExchangeAsync(channel, message.Destination, timeout.Token).ConfigureAwait(false);
            await channel.BasicPublishAsync(
                message.Destination,
                message.MessageName,
                mandatory: true,
                RabbitMqMessageMapper.ToProperties(message),
                message.Body,
                timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            discard = true;
            throw new TimeoutException($"Publishing to exchange '{message.Destination}' was not confirmed within {_options.PublishTimeout}.", ex);
        }
        catch (Exception ex) when (RabbitMqErrors.IsPermanent(ex, _options.DeadLetterUnroutable))
        {
            _declaredExchanges.TryRemove(message.Destination, out _);
            throw new PermanentDeliveryException(RabbitMqErrors.Describe(ex, message.Destination, message.MessageName), ex);
        }
        catch (Exception ex) when (RabbitMqErrors.IsUnroutable(ex))
        {
            throw new InvalidOperationException(RabbitMqErrors.Describe(ex, message.Destination, message.MessageName), ex);
        }
        catch (OperationCanceledException)
        {
            discard = true;
            throw;
        }
        finally
        {
            await _channels.ReturnAsync(channel, discard).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync() => _channels.DisposeAsync();

    private async Task EnsureExchangeAsync(IChannel channel, string exchange, CancellationToken cancellationToken)
    {
        if (!_options.AutoProvision || _declaredExchanges.ContainsKey(exchange))
        {
            return;
        }

        await RabbitMqTopology.DeclareExchangeAsync(channel, exchange, cancellationToken).ConfigureAwait(false);
        if (_declaredExchanges.TryAdd(exchange, true))
        {
            LogExchangeDeclared(exchange);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Declared RabbitMQ exchange {Exchange}.")]
    private partial void LogExchangeDeclared(string exchange);
}
