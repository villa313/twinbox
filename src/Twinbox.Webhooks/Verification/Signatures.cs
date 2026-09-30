using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.AspNetCore.Http;

namespace Twinbox.Webhooks.Verification;

internal static class Signatures
{
    /// <summary>Checks every key against every candidate without stopping early, so timing reveals nothing about which matched.</summary>
    public static bool AnyMatch(
        IEnumerable<byte[]> keys,
        HashAlgorithmName algorithm,
        ReadOnlySpan<byte> signedPrefix,
        ReadOnlySpan<byte> body,
        IReadOnlyList<byte[]> candidates)
    {
        var matched = false;
        foreach (var key in keys)
        {
            using var hmac = IncrementalHash.CreateHMAC(algorithm, key);
            hmac.AppendData(signedPrefix);
            hmac.AppendData(body);
            var expected = hmac.GetHashAndReset();
            foreach (var candidate in candidates)
            {
                matched |= CryptographicOperations.FixedTimeEquals(expected, candidate);
            }
        }

        return matched;
    }

    /// <summary>Null when the header is absent or repeated; a repeated signature header is ambiguous, so it isn't guessed at.</summary>
    public static string? Header(IHeaderDictionary headers, string name) =>
        headers.TryGetValue(name, out var values) && values.Count == 1 && !string.IsNullOrEmpty(values[0]) ? values[0] : null;

    public static bool IsFresh(long unixSeconds, DateTimeOffset now, TimeSpan tolerance)
    {
        var nowSeconds = now.ToUnixTimeSeconds();
        var skew = (long)tolerance.TotalSeconds;
        return unixSeconds >= nowSeconds - skew && unixSeconds <= nowSeconds + skew;
    }

    public static byte[]? FromHex(ReadOnlySpan<char> hex)
    {
        if (hex.Length == 0 || hex.Length % 2 != 0)
        {
            return null;
        }

        var bytes = new byte[hex.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            var high = HexValue(hex[2 * i]);
            var low = HexValue(hex[(2 * i) + 1]);
            if (high < 0 || low < 0)
            {
                return null;
            }

            bytes[i] = (byte)((high << 4) | low);
        }

        return bytes;
    }

    public static byte[]? FromBase64(string value)
    {
        var buffer = new byte[(value.Length * 3 / 4) + 3];
        return Convert.TryFromBase64String(value, buffer, out var written) && written > 0 ? buffer[..written] : null;
    }

    /// <summary>Reads the top-level <c>id</c> and <c>type</c> strings of a JSON object body; nulls when the body isn't one.</summary>
    public static (string? Id, string? Type) ReadIdAndType(ReadOnlySpan<byte> json)
    {
        string? id = null;
        string? type = null;
        try
        {
            var reader = new Utf8JsonReader(json);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                return (null, null);
            }

            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                var isId = reader.ValueTextEquals("id"u8);
                var isType = reader.ValueTextEquals("type"u8);
                reader.Read();
                if (reader.TokenType == JsonTokenType.String)
                {
                    if (isId)
                    {
                        id = reader.GetString();
                    }
                    else if (isType)
                    {
                        type = reader.GetString();
                    }
                }
                else
                {
                    reader.Skip();
                }
            }
        }
        catch (JsonException)
        {
            return (null, null);
        }

        return (id, type);
    }

    public static string BodyHash(ReadOnlySpan<byte> body) => Convert.ToHexString(SHA256.HashData(body));

    private static int HexValue(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _ => -1,
    };
}
