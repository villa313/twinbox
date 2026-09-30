using Microsoft.Extensions.Options;

namespace Twinbox.Http;

internal sealed class HttpOptionsValidator : IValidateOptions<HttpOptions>
{
    public ValidateOptionsResult Validate(string? name, HttpOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var failures = new List<string>();
        foreach (var (endpointName, endpoint) in options.Endpoints)
        {
            var prefix = $"{HttpOptions.SectionName}:Endpoints:{endpointName}";
            if (endpoint.Url is not { IsAbsoluteUri: true } url || (url.Scheme != Uri.UriSchemeHttp && url.Scheme != Uri.UriSchemeHttps))
            {
                failures.Add($"{prefix}:Url must be an absolute http or https URL.");
            }

            if (endpoint.Method is null)
            {
                failures.Add($"{prefix}:Method is required.");
            }

            if (endpoint.Timeout is { } timeout && (timeout <= TimeSpan.Zero || timeout.TotalMilliseconds > int.MaxValue))
            {
                failures.Add($"{prefix}:Timeout must be positive.");
            }

            if (endpoint.WebhookSecret is { } secret && !WebhookSigner.TryParseSecret(secret, out _))
            {
                failures.Add($"{prefix}:WebhookSecret must be \"whsec_\" followed by base64.");
            }

            if (!string.IsNullOrEmpty(endpoint.IdempotencyKeyHeader) && !HttpRequests.IsSendable(endpoint.IdempotencyKeyHeader, "x"))
            {
                failures.Add($"{prefix}:IdempotencyKeyHeader is not a valid header name.");
            }

            // Names only: a header value may be a secret.
            foreach (var (header, value) in endpoint.Headers)
            {
                if (!HttpRequests.IsSendable(header, value ?? string.Empty))
                {
                    failures.Add($"{prefix}:Headers:{header} is not a valid header name or value.");
                }
            }
        }

        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }
}
