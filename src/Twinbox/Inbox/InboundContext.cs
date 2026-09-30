namespace Twinbox.Inbox;

/// <summary>The message being handled on this async flow, so messages sent from a handler inherit its correlation.</summary>
internal static class InboundContext
{
    private static readonly AsyncLocal<MessageContext?> CurrentContext = new();

    public static MessageContext? Current => CurrentContext.Value;

    public static Restore Enter(MessageContext? context)
    {
        var previous = CurrentContext.Value;
        CurrentContext.Value = context;
        return new Restore(previous);
    }

    internal readonly struct Restore(MessageContext? previous) : IDisposable
    {
        public void Dispose() => CurrentContext.Value = previous;
    }
}
