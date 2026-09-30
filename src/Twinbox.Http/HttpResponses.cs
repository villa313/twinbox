using System.Text;

namespace Twinbox.Http;

internal enum HttpOutcome
{
    Delivered,
    Transient,
    Permanent,
}

internal static class HttpResponses
{
    public const int MaxSnippetLength = 500;

    // Enough bytes for 500 characters of multi-byte text without buffering a large error page.
    private const int MaxSnippetBytes = 4 * MaxSnippetLength;

    public static HttpOutcome Classify(int statusCode, HttpEndpointOptions endpoint)
    {
        if (endpoint.SuccessStatusCodes.Contains(statusCode))
        {
            return HttpOutcome.Delivered;
        }

        if (endpoint.TransientStatusCodes.Contains(statusCode))
        {
            return HttpOutcome.Transient;
        }

        // 401 and 403 stay transient: a rotated key or fixed permission should release the backlog, not find it dead-lettered.
        return statusCode switch
        {
            >= 200 and < 300 => HttpOutcome.Delivered,
            401 or 403 or 408 or 429 or >= 500 => HttpOutcome.Transient,
            _ => HttpOutcome.Permanent,
        };
    }

    /// <summary>The wait the server asked for, as delta seconds or an HTTP date; never negative.</summary>
    public static TimeSpan? RetryAfter(HttpResponseMessage response, DateTimeOffset now)
    {
        var header = response.Headers.RetryAfter;
        var delay = header?.Delta ?? (header?.Date is { } date ? date - now : null);
        return delay is { } value && value < TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    /// <summary>The start of the response body on one line, for error messages; an unreadable body gives an empty one.</summary>
    public static async Task<string> ReadSnippetAsync(HttpContent? content, CancellationToken cancellationToken)
    {
        if (content is null)
        {
            return string.Empty;
        }

        try
        {
            var stream = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            await using (stream.ConfigureAwait(false))
            {
                var buffer = new byte[MaxSnippetBytes];
                var read = 0;
                int count;
                while (read < buffer.Length && (count = await stream.ReadAsync(buffer.AsMemory(read), cancellationToken).ConfigureAwait(false)) > 0)
                {
                    read += count;
                }

                return Sanitize(Encoding.UTF8.GetString(buffer, 0, read));
            }
        }
        catch (Exception ex) when ((ex is IOException or HttpRequestException or InvalidOperationException) && !cancellationToken.IsCancellationRequested)
        {
            // The status code already decides what happens; a body that breaks off mid-read only loses detail.
            return string.Empty;
        }
    }

    /// <summary>Collapses control characters and whitespace so the text is safe in logs and a stored error column.</summary>
    public static string Sanitize(string text)
    {
        var builder = new StringBuilder(Math.Min(text.Length, MaxSnippetLength + 1));
        var pendingSpace = false;
        foreach (var c in text)
        {
            if (char.IsControl(c) || char.IsWhiteSpace(c) || c == '�')
            {
                pendingSpace = builder.Length > 0;
                continue;
            }

            if (pendingSpace)
            {
                builder.Append(' ');
                pendingSpace = false;
            }

            builder.Append(c);
            if (builder.Length > MaxSnippetLength)
            {
                break;
            }
        }

        return builder.Length > MaxSnippetLength ? string.Concat(builder.ToString(0, MaxSnippetLength - 1), "…") : builder.ToString();
    }
}
