namespace Twinbox.InMemory;

public sealed class InMemoryOptions
{
    /// <summary>Deliver sent messages to handlers in the background; turn off to pump them manually in tests.</summary>
    public bool AutoDeliver { get; set; } = true;
}
