using System.Diagnostics;
using Twinbox.Transport;

namespace Twinbox.Diagnostics;

/// <summary>The W3C baggage header: comma-separated key=value pairs with percent-encoded values.</summary>
internal static class W3CBaggage
{
    public static string? Encode(IEnumerable<KeyValuePair<string, string?>> baggage)
    {
        var pairs = baggage
            .Where(item => !string.IsNullOrEmpty(item.Key))
            .Select(item => $"{Uri.EscapeDataString(item.Key)}={Uri.EscapeDataString(item.Value ?? string.Empty)}")
            .ToArray();
        return pairs.Length == 0 ? null : string.Join(',', pairs);
    }

    /// <summary>Malformed entries are skipped: baggage is advisory and must never fail a delivery.</summary>
    public static void Restore(Activity? activity, IReadOnlyDictionary<string, string> headers)
    {
        if (activity is null || !headers.TryGetValue(TransportHeaders.Baggage, out var header))
        {
            return;
        }

        // Activity.Baggage lists the most recently added item first, so add in reverse to keep the sender's order.
        foreach (var entry in Enumerable.Reverse(header.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)))
        {
            var member = entry.Split(';', 2)[0];
            var separator = member.IndexOf('=', StringComparison.Ordinal);
            if (separator > 0)
            {
                activity.AddBaggage(
                    Uri.UnescapeDataString(member[..separator].Trim()),
                    Uri.UnescapeDataString(member[(separator + 1)..].Trim()));
            }
        }
    }
}
