namespace Twinbox.Serialization;

public interface IMessageSerializer
{
    string ContentType { get; }

    byte[] Serialize<TMessage>(TMessage message);

    object Deserialize(ReadOnlySpan<byte> body, Type messageType);
}
