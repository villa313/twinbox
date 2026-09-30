using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Twinbox.Http;

/// <summary>Signs requests the Standard Webhooks way: HMAC-SHA256 over "{id}.{timestamp}.{body}".</summary>
internal sealed class WebhookSigner
{
    public const string IdHeader = "webhook-id";
    public const string TimestampHeader = "webhook-timestamp";
    public const string SignatureHeader = "webhook-signature";

    private const string SecretPrefix = "whsec_";

    private readonly byte[] _key;

    private WebhookSigner(byte[] key)
    {
        _key = key;
    }

    public static WebhookSigner? Create(string? secret)
    {
        if (secret is null)
        {
            return null;
        }

        return TryParseSecret(secret, out var key)
            ? new WebhookSigner(key)
            : throw new ArgumentException("The webhook secret must be \"whsec_\" followed by base64.", nameof(secret));
    }

    public static bool TryParseSecret(string secret, out byte[] key)
    {
        key = [];
        if (!secret.StartsWith(SecretPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var encoded = secret[SecretPrefix.Length..];
        var buffer = new byte[encoded.Length];
        if (encoded.Length == 0 || !Convert.TryFromBase64String(encoded, buffer, out var written))
        {
            return false;
        }

        key = buffer[..written];
        return true;
    }

    public void AddHeaders(HttpRequestMessage request, string messageId, DateTimeOffset now, ReadOnlySpan<byte> body)
    {
        var timestamp = now.ToUnixTimeSeconds();
        request.Headers.TryAddWithoutValidation(IdHeader, messageId);
        request.Headers.TryAddWithoutValidation(TimestampHeader, timestamp.ToString(CultureInfo.InvariantCulture));
        request.Headers.TryAddWithoutValidation(SignatureHeader, Sign(messageId, timestamp, body));
    }

    public string Sign(string messageId, long timestamp, ReadOnlySpan<byte> body)
    {
        using var hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, _key);
        hmac.AppendData(Encoding.UTF8.GetBytes(string.Create(CultureInfo.InvariantCulture, $"{messageId}.{timestamp}.")));
        hmac.AppendData(body);
        return "v1," + Convert.ToBase64String(hmac.GetHashAndReset());
    }
}
