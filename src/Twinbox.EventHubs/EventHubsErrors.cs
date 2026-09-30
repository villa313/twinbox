using Azure.Messaging.EventHubs;

namespace Twinbox.EventHubs;

internal static class EventHubsErrors
{
    // Access errors stay transient: an RBAC or key fix should release the backlog rather than find it dead-lettered.
    public static bool IsPermanent(Exception error) =>
        error is EventHubsException { Reason: EventHubsException.FailureReason.ResourceNotFound or EventHubsException.FailureReason.MessageSizeExceeded };
}
