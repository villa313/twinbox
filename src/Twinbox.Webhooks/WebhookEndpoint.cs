using System.Security.Cryptography;
using System.Text;

namespace Twinbox.Webhooks;

/// <summary>A mapped endpoint's settings. <see cref="Prototype"/> is cloned with <c>with</c>, which keeps the endpoint's derived type.</summary>
internal sealed record WebhookEndpoint(
    string Provider,
    IWebhookVerifier Verifier,
    IReadOnlyList<string> ForwardedHeaders,
    long MaxBodySize,
    string MessageName,
    Type MessageType,
    WebhookReceived Prototype)
{
    public string Destination => "webhooks/" + Provider;

    /// <summary>Deterministic, so a provider's retry of an event maps to the row already stored instead of a second one.</summary>
    public Guid MessageIdFor(string eventId)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes($"twinbox-webhook:{MessageName}:{Provider}:{eventId}"), hash);
        var bytes = hash[..16];
        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x80);
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes, bigEndian: true);
    }
}
