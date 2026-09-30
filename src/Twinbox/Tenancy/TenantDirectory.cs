namespace Twinbox.Tenancy;

/// <summary>Lists tenants to work through, caching the list so every dispatch pass doesn't query it.</summary>
public sealed class TenantDirectory(TwinboxScopeFactory scopes, TimeProvider time, TenancyOptions? tenancy = null) : IDisposable
{
    private static readonly IReadOnlyCollection<string?> NoTenants = [null];

    private readonly SemaphoreSlim _gate = new(1, 1);
    private IReadOnlyCollection<string?> _cached = [];
    private DateTimeOffset _expiresAt = DateTimeOffset.MinValue;

    /// <summary>
    /// Runs <paramref name="action"/> once per tenant with that tenant entered, so scopes from
    /// <see cref="TwinboxScopeFactory"/> resolve its services. Runs once with no tenant when tenancy isn't configured.
    /// </summary>
    public async Task ForEachTenantAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        foreach (var tenant in await GetTenantsAsync(cancellationToken).ConfigureAwait(false))
        {
            using var _ = TenantScope.Enter(tenant);
            await action(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Makes <paramref name="tenant"/> current until disposed, so Twinbox scopes and stores resolve its services.</summary>
    public static IDisposable Enter(string? tenant) => TenantScope.Enter(tenant);

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
