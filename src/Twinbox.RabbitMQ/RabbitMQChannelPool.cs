using System.Collections.Concurrent;
using RabbitMQ.Client;

namespace Twinbox.RabbitMQ;

/// <summary>Publisher-confirm channels handed out one caller at a time, since a channel is not safe for concurrent publishes.</summary>
internal sealed class RabbitMQChannelPool(RabbitMQConnection connection, int size) : IAsyncDisposable
{
    private readonly ConcurrentBag<IChannel> _idle = [];
    private readonly SemaphoreSlim _slots = new(Math.Max(1, size), Math.Max(1, size));

    public async Task<IChannel> RentAsync(CancellationToken cancellationToken)
    {
        await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            while (_idle.TryTake(out var idle))
            {
                if (idle.IsOpen)
                {
                    return idle;
                }

                await DiscardAsync(idle).ConfigureAwait(false);
            }

            var open = await connection.GetAsync(cancellationToken).ConfigureAwait(false);
            return await open.CreateChannelAsync(
                new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _slots.Release();
            throw;
        }
    }

    /// <summary>Discard channels whose state is unknown, e.g. after a timed-out publish still awaiting its confirm.</summary>
    public async ValueTask ReturnAsync(IChannel channel, bool discard)
    {
        if (discard || !channel.IsOpen)
        {
            await DiscardAsync(channel).ConfigureAwait(false);
        }
        else
        {
            _idle.Add(channel);
        }

        _slots.Release();
    }

    public async ValueTask DisposeAsync()
    {
        while (_idle.TryTake(out var channel))
        {
            await DiscardAsync(channel).ConfigureAwait(false);
        }
    }

    private static async ValueTask DiscardAsync(IChannel channel)
    {
        try
        {
            if (channel.IsOpen)
            {
                await channel.CloseAsync().ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            // The channel is being thrown away; a failed close changes nothing.
        }

        await channel.DisposeAsync().ConfigureAwait(false);
    }
}
