using StackExchange.Redis;
using Twinbox.Transport;

namespace Twinbox.RedisStreams;

/// <summary>Appends each message to the stream named by its destination.</summary>
public sealed class RedisStreamsTransport : ITransport
{
    public const string TransportName = "redisstreams";

    private readonly RedisStreamsConnection _connection;

    internal RedisStreamsTransport(RedisStreamsConnection connection)
    {
        _connection = connection;
    }

    public string Name => TransportName;

    public async Task SendAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        var database = await _connection.GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await database.StreamAddAsync(
                message.Destination,
                RedisStreamsMapping.ToEntry(message),
                maxLength: _connection.Options.MaxLength,
                useApproximateMaxLength: true).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (RedisStreamsErrors.IsPermanent(ex))
        {
            throw new PermanentDeliveryException($"Redis rejected message {message.MessageId} for stream '{message.Destination}': {ex.Message}", ex);
        }
    }
}
