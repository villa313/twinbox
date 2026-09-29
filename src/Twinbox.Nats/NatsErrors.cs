using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Twinbox.Nats;

internal static class NatsErrors
{
    public const int StreamNameInUse = 10058;
    public const int MessageTooLarge = 10054;

    /// <summary>Rejections a retry cannot fix, so the outbox dead-letters instead of backing off.</summary>
    public static bool IsPermanent(Exception error) => error switch
    {
        NatsJSApiException { Error: var apiError } => IsPermanent(apiError),
        NatsServerException { Error: var text } => text.Contains("permissions violation", StringComparison.OrdinalIgnoreCase),

        // Newer clients throw a dedicated subtype, older ones a plain NatsException; both carry this text.
        NatsException { Message: var text } => text.Contains("exceeds server's maximum payload size", StringComparison.OrdinalIgnoreCase),
        _ => false,
    };

    /// <summary>Bad requests and permission denials stay that way; unavailability and full streams may clear.</summary>
    public static bool IsPermanent(ApiError error) => error.ErrCode == MessageTooLarge || error.Code is 400 or 403 or 413;

    /// <summary>No reply to a publish: either no stream captures the subject, or the server was briefly unreachable.</summary>
    public static bool IsNoResponse(Exception error) => error is NatsJSPublishNoResponseException or NatsNoRespondersException;

    public static string Describe(ApiError error) => $"{error.Code}/{error.ErrCode} {error.Description}";
}
