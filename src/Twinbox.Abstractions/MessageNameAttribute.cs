namespace Twinbox;

/// <summary>Stable wire name for a message type, so renaming or moving the class doesn't break consumers.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class MessageNameAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}
