using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Twinbox.Transport;

namespace Twinbox.RabbitMQ;

internal sealed partial class RabbitMqConsumerService(
    RabbitMqConnection connection,
    IInboundPipeline pipeline,
    IOptions<RabbitMqOptions> options,
    ILogger<RabbitMqConsumerService> logger) : BackgroundService
{
    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    private readonly ILogger _logger = logger;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly List<IChannel> _channels = [];

    private enum Settlement
    {
        Ack,
        DeadLetter,
        Requeue,
    }

    /// <summary>Completes once every listener is consuming.</summary>
    public Task Ready => _ready.Task;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        if (settings.Listeners.Count == 0)
        {
            _ready.TrySetResult();
            return;
        }

        try
        {
            var open = await connection.WaitForConnectionAsync(stoppingToken).ConfigureAwait(false);
            foreach (var listener in settings.Listeners)
            {
                _channels.Add(await StartWithRetryAsync(open, listener, settings, stoppingToken).ConfigureAwait(false));
            }

            _ready.TrySetResult();
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _ready.TrySetCanceled(stoppingToken);
        }
        finally
        {
            await CloseChannelsAsync().ConfigureAwait(false);
        }
    }

    private async Task<IChannel> StartWithRetryAsync(IConnection open, RabbitMqListener listener, RabbitMqOptions settings, CancellationToken stoppingToken)
    {
        var delay = InitialRetryDelay;
        while (true)
        {
            try
            {
                return await StartAsync(open, listener, settings, stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                LogListenerFailed(ex, listener.Queue, delay);
            }

            await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRetryDelay.Ticks));
        }
    }

    private async Task<IChannel> StartAsync(IConnection open, RabbitMqListener listener, RabbitMqOptions settings, CancellationToken stoppingToken)
    {
        var channel = await open.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                consumerDispatchConcurrency: Math.Max((ushort)1, settings.ConsumerConcurrency)),
            stoppingToken).ConfigureAwait(false);
        try
        {
            if (settings.AutoProvision)
            {
                await RabbitMqTopology.DeclareListenerAsync(channel, listener, settings.DeliveryLimit, stoppingToken).ConfigureAwait(false);
            }

            await channel.BasicQosAsync(0, settings.PrefetchCount, global: false, stoppingToken).ConfigureAwait(false);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, delivery) => HandleAsync(channel, listener.Queue, delivery, stoppingToken);
            await channel.BasicConsumeAsync(listener.Queue, autoAck: false, consumer, stoppingToken).ConfigureAwait(false);
            LogListening(listener.Queue, listener.Exchange, listener.BindingKey);
            return channel;
        }
        catch
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task HandleAsync(IChannel channel, string queue, BasicDeliverEventArgs delivery, CancellationToken stoppingToken)
    {
        var settlement = await ProcessAsync(queue, delivery, stoppingToken).ConfigureAwait(false);
        try
        {
            if (settlement == Settlement.Ack)
            {
                await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, CancellationToken.None).ConfigureAwait(false);
            }
            else
            {
                await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: settlement == Settlement.Requeue, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            // The broker requeues unsettled deliveries when the channel closes, and the inbox absorbs the repeat.
            LogSettleFailed(ex, delivery.BasicProperties.MessageId, queue);
        }
    }

    private async Task<Settlement> ProcessAsync(string queue, BasicDeliverEventArgs delivery, CancellationToken stoppingToken)
    {
        try
        {
            var message = RabbitMqMessageMapper.ToIncoming(queue, delivery.BasicProperties, delivery.Body, delivery.Redelivered);
            await pipeline.ProcessAsync(message, stoppingToken).ConfigureAwait(false);
            return Settlement.Ack;
        }
        catch (PermanentDeliveryException ex)
        {
            LogDeadLettered(ex, delivery.BasicProperties.MessageId, queue);
            return Settlement.DeadLetter;
        }
        catch (Exception ex)
        {
            if (!stoppingToken.IsCancellationRequested)
            {
                LogProcessingFailed(ex, delivery.BasicProperties.MessageId, queue);
            }

            return Settlement.Requeue;
        }
    }

    private async Task CloseChannelsAsync()
    {
        foreach (var channel in _channels)
        {
            try
            {
                await channel.CloseAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogCloseFailed(ex);
            }

            await channel.DisposeAsync().ConfigureAwait(false);
        }

        _channels.Clear();
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming RabbitMQ queue {Queue} bound to {Exchange} with '{BindingKey}'.")]
    private partial void LogListening(string queue, string exchange, string bindingKey);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not start consuming RabbitMQ queue {Queue}; retrying in {Delay}.")]
    private partial void LogListenerFailed(Exception error, string queue, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Queue} failed permanently; dead-lettering it.")]
    private partial void LogDeadLettered(Exception error, string? messageId, string queue);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageId} from {Queue} failed; returning it to the queue.")]
    private partial void LogProcessingFailed(Exception error, string? messageId, string queue);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not settle message {MessageId} from {Queue}; the broker will redeliver it.")]
    private partial void LogSettleFailed(Exception error, string? messageId, string queue);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Closing a RabbitMQ consumer channel failed.")]
    private partial void LogCloseFailed(Exception error);
}
