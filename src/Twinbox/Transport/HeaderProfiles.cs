namespace Twinbox.Transport;

internal sealed class HeaderProfiles(IEnumerable<HeaderProfile> profiles)
{
    private readonly HeaderProfile[] _profiles = [.. profiles];

    public bool IsEmpty => _profiles.Length == 0;

    public void Write(IDictionary<string, string> headers, string messageId, string messageName, string? partitionKey)
    {
        foreach (var profile in _profiles)
        {
            headers[profile.MessageId] = messageId;
            headers[profile.MessageName] = messageName;
            if (profile.PartitionKey is not null && partitionKey is not null)
            {
                headers[profile.PartitionKey] = partitionKey;
            }
        }
    }

    /// <summary>
    /// A profile's header wins over what the transport derived, because a transport may have fallen back to a
    /// broker-assigned id that changes on redelivery.
    /// </summary>
    public IncomingMessage Read(IncomingMessage message)
    {
        foreach (var profile in _profiles)
        {
            if (message.Headers.TryGetValue(profile.MessageId, out var id) && !string.IsNullOrEmpty(id))
            {
                message.Headers.TryGetValue(profile.MessageName, out var name);
                string? partitionKey = null;
                if (profile.PartitionKey is not null)
                {
                    message.Headers.TryGetValue(profile.PartitionKey, out partitionKey);
                }

                return message with
                {
                    MessageId = id,
                    MessageName = string.IsNullOrEmpty(name) ? message.MessageName : name,
                    PartitionKey = partitionKey ?? message.PartitionKey,
                };
            }
        }

        return message;
    }
}
