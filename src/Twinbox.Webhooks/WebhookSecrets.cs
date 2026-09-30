using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Twinbox.Webhooks;

/// <summary>Signing secrets for an endpoint. All of them are accepted, so a rotation can overlap old and new secrets.</summary>
public sealed class WebhookSecrets
{
    private readonly string[]? _literal;
    private readonly string? _configurationKey;

    private WebhookSecrets(string[]? literal, string? configurationKey)
    {
        _literal = literal;
        _configurationKey = configurationKey;
    }

    public static WebhookSecrets Of(params string[] secrets)
    {
        ArgumentNullException.ThrowIfNull(secrets);
        if (secrets.Length == 0 || secrets.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("Pass at least one non-empty secret.", nameof(secrets));
        }

        return new WebhookSecrets([.. secrets], null);
    }

    /// <summary>Read on every request, so a reload picks up a rotation. The key holds one secret or an array (<c>Key:0</c>, <c>Key:1</c>).</summary>
    public static WebhookSecrets FromConfiguration(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return new WebhookSecrets(null, key);
    }

    /// <exception cref="InvalidOperationException">No secret is configured; webhooks are refused rather than accepted unsigned.</exception>
    public IReadOnlyList<string> Resolve(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (_literal is not null)
        {
            return _literal;
        }

        var section = services.GetRequiredService<IConfiguration>().GetSection(_configurationKey!);
        string[] secrets = string.IsNullOrWhiteSpace(section.Value)
            ? [.. section.GetChildren().Select(c => c.Value).Where(v => !string.IsNullOrWhiteSpace(v)).Select(v => v!)]
            : [section.Value];

        return secrets.Length > 0
            ? secrets
            : throw new InvalidOperationException($"No webhook secret is configured at '{_configurationKey}'.");
    }
}
