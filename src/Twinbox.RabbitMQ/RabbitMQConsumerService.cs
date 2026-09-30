using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Twinbox.Transport;

namespace Twinbox.RabbitMQ;

internal sealed partial class RabbitMQConsumerService(
    RabbitMQConnection connection,
    IInboundPipeline pipeline,
    IOptions<RabbitMQOptions> options,
    ILogger<RabbitMQConsumerService> logger) : BackgroundService
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
        RequeueLater,
    }

    /// <summary>Completes once every listener is consuming.</summary>
    public Task Ready => _ready.Task;

    internal static TimeSpan RetryDelay(RabbitMQOptions options, int attempt) =>
        RetryBackoff.Delay(options.RetryDelay, options.MaxRetryDelay, attempt);

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

    private async Task<IChannel> StartWithRetryAsync(IConnection open, RabbitMQListener listener, RabbitMQOptions settings, CancellationToken stoppingToken)
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

    private async Task<IChannel> StartAsync(IConnection open, RabbitMQListener listener, RabbitMQOptions settings, CancellationToken stoppingToken)
    {
        var channel = await open.CreateChannelAsync(
            new CreateChannelOptions(
                publisherConfirmationsEnabled: false,
                publisherConfirmationTrackingEnabled: false,
                consumerDispatchConcurrency: (ushort)settings.ConsumerConcurrency),
            stoppingToken).ConfigureAwait(false);
        try
        {
            if (settings.AutoProvision)
            {
                await RabbitMQTopology.DeclareListenerAsync(channel, listener, settings.MaxDeliveryAttempts, stoppingToken).ConfigureAwait(false);
            }

            await channel.BasicQosAsync(0, (ushort)settings.PrefetchCount, global: false, stoppingToken).ConfigureAwait(false);
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
        var message = RabbitMQMessageMapper.ToIncoming(queue, delivery.BasicProperties, delivery.Body, delivery.Redelivered);
        var settlement = await ProcessAsync(message, stoppingToken).ConfigureAwait(false);
        if (settlement == Settlement.RequeueLater)
        {
            // Returns straight away so the channel keeps dispatching while this delivery waits out its backoff.
            _ = RequeueLaterAsync(channel, message, delivery.DeliveryTag, RetryDelay(options.Value, message.DeliveryAttempt), stoppingToken);
            return;
        }

        await SettleAsync(channel, message, delivery.DeliveryTag, settlement).ConfigureAwait(false);
    }

    private async Task<Settlement> ProcessAsync(IncomingMessage message, CancellationToken stoppingToken)
    {
        try
        {
            await pipeline.ProcessAsync(message, stoppingToken).ConfigureAwait(false);
            return Settlement.Ack;
        }
        catch (PermanentDeliveryException ex)
        {
            LogDeadLettered(ex, message.MessageId, message.Source);
            InboundDiagnostics.RecordDeadLettered(RabbitMQTransport.TransportName, message.Source);
            return Settlement.DeadLetter;
        }
        catch (Exception) when (stoppingToken.IsCancellationRequested)
        {
            return Settlement.Requeue;
        }
        catch (Exception ex) when (message.DeliveryAttempt >= options.Value.MaxDeliveryAttempts)
        {
            LogDeliveriesExhausted(ex, message.MessageId, message.Source, message.DeliveryAttempt);
            InboundDiagnostics.RecordDeadLettered(RabbitMQTransport.TransportName, message.Source);
            return Settlement.DeadLetter;
        }
        catch (Exception ex)
        {
            LogProcessingFailed(ex, message.MessageId, message.Source, message.DeliveryAttempt, RetryDelay(options.Value, message.DeliveryAttempt));
            return Settlement.RequeueLater;
        }
    }

    // The delivery stays unacknowledged while it waits; if the channel closes first, the broker requeues it anyway.
    private async Task RequeueLaterAsync(IChannel channel, IncomingMessage message, ulong deliveryTag, TimeSpan delay, CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        await SettleAsync(channel, message, deliveryTag, Settlement.Requeue).ConfigureAwait(false);
    }

    private async Task SettleAsync(IChannel channel, IncomingMessage message, ulong deliveryTag, Settlement settlement)
    {
        try
        {
            if (settlement == Settlement.Ack)
            {
                await channel.BasicAckAsync(deliveryTag, multiple: false, CancellationToken.None).ConfigureAwait(false);
            }
            else
            {
                await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: settlement == Settlement.Requeue, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            // The broker requeues unsettled deliveries when the channel closes, and the inbox absorbs the repeat.
            LogSettleFailed(ex, message.MessageId, message.Source);
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

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Queue} failed permanently; rejecting it to the queue's dead-letter exchange.")]
    private partial void LogDeadLettered(Exception error, string messageId, string queue);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} from {Queue} failed on its last allowed delivery ({Attempt}); rejecting it to the queue's dead-letter exchange.")]
    private partial void LogDeliveriesExhausted(Exception error, string messageId, string queue, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageId} from {Queue} failed on attempt {Attempt}; returning it to the queue in {Delay}.")]
    private partial void LogProcessingFailed(Exception error, string messageId, string queue, int attempt, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not settle message {MessageId} from {Queue}; the broker will redeliver it.")]
    private partial void LogSettleFailed(Exception error, string messageId, string queue);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Closing a RabbitMQ consumer channel failed.")]
    private partial void LogCloseFailed(Exception error);
}
