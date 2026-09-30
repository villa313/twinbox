using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Twinbox.Webhooks.Verification;

/// <summary>Standard Webhooks (and Svix's svix-* headers): base64 HMAC-SHA256 of "{id}.{timestamp}.{body}", so the id is signed too.</summary>
internal sealed class StandardWebhooksVerifier(WebhookSecrets secrets, TimeSpan tolerance) : IWebhookVerifier
{
    private const string SecretPrefix = "whsec_";

    public string DefaultProvider => "standard-webhooks";

    public WebhookVerification Verify(WebhookRequest request)
    {
        var headerPrefix = request.Headers.ContainsKey("webhook-id") ? "webhook-" : "svix-";
        var id = Signatures.Header(request.Headers, headerPrefix + "id");
        var timestampText = Signatures.Header(request.Headers, headerPrefix + "timestamp");
        var signatureList = Signatures.Header(request.Headers, headerPrefix + "signature");
        if (id is null || timestampText is null || signatureList is null)
        {
            return WebhookVerification.Unauthorized("missing webhook-id, webhook-timestamp or webhook-signature header");
        }

        if (!long.TryParse(timestampText, NumberStyles.None, CultureInfo.InvariantCulture, out var timestamp))
        {
            return WebhookVerification.Unauthorized("malformed webhook-timestamp header");
        }

        if (!Signatures.IsFresh(timestamp, request.ReceivedAt, tolerance))
        {
            return WebhookVerification.Unauthorized("signature timestamp outside the tolerance");
        }

        var candidates = new List<byte[]>();
        foreach (var entry in signatureList.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            // Other versions (v1a is asymmetric) aren't supported and are skipped rather than failing the whole list.
            if (entry.StartsWith("v1,", StringComparison.Ordinal) && Signatures.FromBase64(entry[3..]) is { } signature)
            {
                candidates.Add(signature);
            }
        }

        if (candidates.Count == 0)
        {
            return WebhookVerification.Unauthorized("no v1 signature");
        }

        var prefix = Encoding.UTF8.GetBytes(string.Create(CultureInfo.InvariantCulture, $"{id}.{timestamp}."));
        var keys = secrets.Resolve(request.Services).Select(DecodeSecret);
        if (!Signatures.AnyMatch(keys, HashAlgorithmName.SHA256, prefix, request.Body.Span, candidates))
        {
            return WebhookVerification.Unauthorized("signature mismatch");
        }

        return WebhookVerification.Verified(id, Signatures.ReadIdAndType(request.Body.Span).Type);
    }

    private static byte[] DecodeSecret(string secret)
    {
        var encoded = secret.StartsWith(SecretPrefix, StringComparison.Ordinal) ? secret[SecretPrefix.Length..] : secret;
        return Signatures.FromBase64(encoded)
            ?? throw new InvalidOperationException("A Standard Webhooks secret must be base64, optionally prefixed with 'whsec_'.");
    }
}
