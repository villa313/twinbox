using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Twinbox.Webhooks.Verification;

/// <summary><c>Stripe-Signature: t=..., v1=...</c>: hex HMAC-SHA256 of "{t}.{body}"; the event id and type come from the signed body.</summary>
internal sealed class StripeVerifier(WebhookSecrets secrets, TimeSpan tolerance) : IWebhookVerifier
{
    public string DefaultProvider => "stripe";

    public WebhookVerification Verify(WebhookRequest request)
    {
        if (Signatures.Header(request.Headers, "Stripe-Signature") is not { } header)
        {
            return WebhookVerification.Unauthorized("missing Stripe-Signature header");
        }

        long? timestamp = null;
        var candidates = new List<byte[]>();
        foreach (var part in header.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = part.IndexOf('=', StringComparison.Ordinal);
            if (separator <= 0)
            {
                continue;
            }

            var value = part[(separator + 1)..];
            switch (part[..separator])
            {
                case "t" when long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var t):
                    timestamp = t;
                    break;
                case "v1" when Signatures.FromHex(value) is { } signature:
                    candidates.Add(signature);
                    break;
            }
        }

        if (timestamp is not { } signedAt || candidates.Count == 0)
        {
            return WebhookVerification.Unauthorized("malformed Stripe-Signature header");
        }

        if (!Signatures.IsFresh(signedAt, request.ReceivedAt, tolerance))
        {
            return WebhookVerification.Unauthorized("signature timestamp outside the tolerance");
        }

        var prefix = Encoding.UTF8.GetBytes(signedAt.ToString(CultureInfo.InvariantCulture) + ".");
        var keys = secrets.Resolve(request.Services).Select(Encoding.UTF8.GetBytes);
        if (!Signatures.AnyMatch(keys, HashAlgorithmName.SHA256, prefix, request.Body.Span, candidates))
        {
            return WebhookVerification.Unauthorized("signature mismatch");
        }

        var (id, type) = Signatures.ReadIdAndType(request.Body.Span);
        return id is null
            ? WebhookVerification.Malformed("the event has no id")
            : WebhookVerification.Verified(id, type);
    }
}
