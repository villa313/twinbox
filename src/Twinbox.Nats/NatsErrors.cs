using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Twinbox.Nats;

internal static class NatsErrors
{
    public const int StreamNameInUse = 10058;
    public const int MessageTooLarge = 10054;

    /// <summary>Rejections a retry cannot fix, so the outbox dead-letters instead of backing off. Permission errors stay
    /// transient: fixing the user's permissions should release the backlog rather than find it dead-lettered.</summary>
    public static bool IsPermanent(Exception error) => error switch
    {
        NatsJSApiException { Error: var apiError } => IsPermanent(apiError),

        // Newer clients throw a dedicated subtype, older ones a plain NatsException; both carry this text.
        NatsException { Message: var text } => text.Contains("exceeds server's maximum payload size", StringComparison.OrdinalIgnoreCase),
        _ => false,
    };

    /// <summary>Bad requests stay that way; unavailability, full streams and permission denials may clear.</summary>
    public static bool IsPermanent(ApiError error) => error.ErrCode == MessageTooLarge || error.Code is 400 or 413;

    /// <summary>No reply to a publish: either no stream captures the subject, or the server was briefly unreachable.</summary>
    public static bool IsNoResponse(Exception error) => error is NatsJSPublishNoResponseException or NatsNoRespondersException;

    public static string Describe(ApiError error) => $"{error.Code}/{error.ErrCode} {error.Description}";
}
