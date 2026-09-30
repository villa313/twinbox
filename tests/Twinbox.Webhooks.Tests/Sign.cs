using System.Security.Cryptography;
using System.Text;

namespace Twinbox.Webhooks.Tests;

/// <summary>Signs the way each provider documents it, independently of the code under test.</summary>
internal static class Sign
{
    public static string Stripe(string secret, long timestamp, string body) => $"t={timestamp},v1={StripeV1(secret, timestamp, body)}";

    public static string StripeV1(string secret, long timestamp, string body) =>
        Hex(HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes($"{timestamp}.{body}")));

    public static string Shopify(string secret, string body) =>
        Convert.ToBase64String(HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes(body)));

    public static string GitHub(string secret, string body) =>
        "sha256=" + Hex(HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes(body)));

    public static string StandardWebhooks(string secret, string id, long timestamp, string body) =>
        "v1," + Convert.ToBase64String(HMACSHA256.HashData(
            Convert.FromBase64String(secret["whsec_".Length..]),
            Encoding.UTF8.GetBytes($"{id}.{timestamp}.{body}")));

    public static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();
}
