namespace Twinbox.Tenancy;

/// <summary>Lists tenants to work through, caching the list so every dispatch pass doesn't query it.</summary>
internal sealed class TenantDirectory(TwinboxScopeFactory scopes, TimeProvider time, TenancyOptions? tenancy = null) : IDisposable
{
    private static readonly IReadOnlyCollection<string?> NoTenants = [null];

    private readonly SemaphoreSlim _gate = new(1, 1);
    private IReadOnlyCollection<string?> _cached = [];
    private DateTimeOffset _expiresAt = DateTimeOffset.MinValue;

    /// <summary>Returns a single null entry when tenancy isn't configured, so callers can always loop.</summary>
    public async Task<IReadOnlyCollection<string?>> GetTenantsAsync(CancellationToken cancellationToken)
    {
        if (tenancy?.ListTenants is not { } list)
        {
            return NoTenants;
        }

        var now = time.GetUtcNow();
        if (now < _expiresAt)
        {
            return _cached;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (now >= _expiresAt)
            {
                var scope = scopes.CreateAsyncScope();
                await using (scope.ConfigureAwait(false))
                {
                    _cached = [.. await list(scope.ServiceProvider, cancellationToken).ConfigureAwait(false)];
                }

                _expiresAt = now + tenancy.TenantListCacheDuration;
            }

            return _cached;
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();
}
