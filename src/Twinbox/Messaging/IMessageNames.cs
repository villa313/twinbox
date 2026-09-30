namespace Twinbox;

/// <summary>The wire name of each message type; asking for a type's name also registers it for receiving.</summary>
public interface IMessageNames
{
    string GetName(Type messageType);
}
