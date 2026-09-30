using System.Text;
using Twinbox.Transport;

namespace Twinbox.Http;

/// <summary>Fills "{name}" placeholders in an endpoint URL from the message being sent.</summary>
internal static class UrlTemplate
{
    public static Uri Expand(Uri url, TransportMessage message)
    {
        var template = url.OriginalString;
        if (!template.Contains('{', StringComparison.Ordinal))
        {
            return url;
        }

        var expanded = new StringBuilder(template.Length + 32);
        var position = 0;
        while (position < template.Length)
        {
            var open = template.IndexOf('{', position);
            var close = open < 0 ? -1 : template.IndexOf('}', open + 1);
            if (close < 0)
            {
                expanded.Append(template, position, template.Length - position);
                break;
            }

            expanded.Append(template, position, open - position);
            var name = template[(open + 1)..close];
            var value = Resolve(name, message);
            if (string.IsNullOrEmpty(value))
            {
                // Retrying cannot conjure the value, so the message is dead-lettered at once.
                throw new PermanentDeliveryException($"Message {message.MessageId} has no value for the URL placeholder {{{name}}}.");
            }

            expanded.Append(Uri.EscapeDataString(value));
            position = close + 1;
        }

        return new Uri(expanded.ToString(), UriKind.Absolute);
    }

    private static string? Resolve(string name, TransportMessage message)
    {
        if (name.Equals("messageId", StringComparison.OrdinalIgnoreCase))
        {
            return message.MessageId;
        }

        if (name.Equals("messageName", StringComparison.OrdinalIgnoreCase))
        {
            return message.MessageName;
        }

        if (name.Equals("partitionKey", StringComparison.OrdinalIgnoreCase))
        {
            return message.PartitionKey;
        }

        if (name.Equals("tenant", StringComparison.OrdinalIgnoreCase))
        {
            name = TransportHeaders.TenantId;
        }

        if (message.Headers.TryGetValue(name, out var exact))
        {
            return exact;
        }

        foreach (var (header, value) in message.Headers)
        {
            if (header.Equals(name, StringComparison.OrdinalIgnoreCase))
            {
                return value;
            }
        }

        return null;
    }
}
