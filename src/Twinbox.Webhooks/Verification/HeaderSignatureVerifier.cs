using System.Security.Cryptography;
using System.Text;

namespace Twinbox.Webhooks.Verification;

/// <summary>A body-only HMAC in one header. These schemes sign no timestamp or id, so replays are stopped only by the (unsigned) id.</summary>
internal sealed class HeaderSignatureVerifier(WebhookSecrets secrets, HmacSignatureOptions options, string defaultProvider) : IWebhookVerifier
{
    public string DefaultProvider => defaultProvider;

    public static HeaderSignatureVerifier Shopify(WebhookSecrets secrets) => new(
        secrets,
        new HmacSignatureOptions
        {
            SignatureHeader = "X-Shopify-Hmac-Sha256",
            Encoding = SignatureEncoding.Base64,
            EventIdHeader = "X-Shopify-Webhook-Id",
            EventTypeHeader = "X-Shopify-Topic",
        },
        "shopify");

    public static HeaderSignatureVerifier GitHub(WebhookSecrets secrets) => new(
        secrets,
        new HmacSignatureOptions
        {
            SignatureHeader = "X-Hub-Signature-256",
            Prefix = "sha256=",
            EventIdHeader = "X-GitHub-Delivery",
            EventTypeHeader = "X-GitHub-Event",
        },
        "github");

    public WebhookVerification Verify(WebhookRequest request)
    {
        if (Signatures.Header(request.Headers, options.SignatureHeader) is not { } header)
        {
            return WebhookVerification.Unauthorized($"missing {options.SignatureHeader} header");
        }

        var encoded = header.AsSpan().Trim();
        if (options.Prefix is { Length: > 0 } prefix)
        {
            if (!encoded.StartsWith(prefix, StringComparison.Ordinal))
            {
                return WebhookVerification.Unauthorized($"malformed {options.SignatureHeader} header");
            }

            encoded = encoded[prefix.Length..];
        }

        var signature = options.Encoding == SignatureEncoding.Hex
            ? Signatures.FromHex(encoded)
            : Signatures.FromBase64(encoded.ToString());
        if (signature is null)
        {
            return WebhookVerification.Unauthorized($"malformed {options.SignatureHeader} header");
        }

        var algorithm = options.Algorithm == WebhookHmacAlgorithm.Sha512 ? HashAlgorithmName.SHA512 : HashAlgorithmName.SHA256;
        var keys = secrets.Resolve(request.Services).Select(Encoding.UTF8.GetBytes);
        if (!Signatures.AnyMatch(keys, algorithm, [], request.Body.Span, [signature]))
        {
            return WebhookVerification.Unauthorized("signature mismatch");
        }

        var type = options.EventTypeHeader is { } typeHeader ? Signatures.Header(request.Headers, typeHeader) : null;
        if (options.EventIdHeader is null)
        {
            // Without an id, identical bodies are treated as the same event, which is what a provider retry sends.
            return WebhookVerification.Verified(Signatures.BodyHash(request.Body.Span), type);
        }

        return Signatures.Header(request.Headers, options.EventIdHeader) is { } id
            ? WebhookVerification.Verified(id, type)
            : WebhookVerification.Malformed($"missing {options.EventIdHeader} header");
    }
}
