namespace Twinbox;

/// <summary>What this app's configuration sends and handles, so startup checks and tests can catch a missing route or handler.</summary>
public interface ITwinboxTopology
{
    /// <summary>Destinations a message of this type is sent to, including base-type and interface routes; empty when unrouted.</summary>
    IReadOnlyList<string> DestinationsOf(Type messageType);

    /// <summary>Consumer names of the handlers an incoming message of this type reaches; empty when it would go unhandled.</summary>
    IReadOnlyList<string> ConsumersOf(Type messageType);

    /// <summary>False when an incoming message of this type can't be matched by name, e.g. a subtype only routed by its base.</summary>
    bool IsKnown(Type messageType);
}
