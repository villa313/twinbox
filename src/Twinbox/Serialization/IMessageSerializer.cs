namespace Twinbox.Serialization;

public interface IMessageSerializer
{
    string ContentType { get; }

    byte[] Serialize(object message, Type messageType);

    object Deserialize(ReadOnlySpan<byte> body, Type messageType);
}
