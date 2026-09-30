namespace Twinbox.Webhooks;

/// <summary>A signature header holding an HMAC of the raw body, e.g. <c>X-Signature: sha256=&lt;hex&gt;</c>.</summary>
public sealed class HmacSignatureOptions
{
    public string SignatureHeader { get; set; } = string.Empty;

    public WebhookHmacAlgorithm Algorithm { get; set; } = WebhookHmacAlgorithm.Sha256;

    public SignatureEncoding Encoding { get; set; } = SignatureEncoding.Hex;

    /// <summary>Text before the encoded signature, such as <c>sha256=</c>; matched exactly.</summary>
    public string? Prefix { get; set; }

    /// <summary>Without one, a hash of the body is the event id, so only byte-identical retries are deduplicated.</summary>
    public string? EventIdHeader { get; set; }

    public string? EventTypeHeader { get; set; }
}

public enum WebhookHmacAlgorithm
{
    Sha256,
    Sha512,
}

public enum SignatureEncoding
{
    Hex,
    Base64,
}
