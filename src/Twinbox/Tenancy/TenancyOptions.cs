namespace Twinbox;

/// <summary>
/// Database-per-tenant support: Twinbox lists tenants to dispatch each one's outbox, and enters a tenant on every
/// scope it creates so tenant-aware registrations (e.g. a DbContext's connection string) resolve correctly.
/// </summary>
public sealed class TenancyOptions
{
    public Func<IServiceProvider, CancellationToken, Task<IReadOnlyCollection<string>>>? ListTenants { get; set; }

    /// <summary>Applies a tenant to a fresh scope created by Twinbox, e.g. by setting your scoped tenant context.</summary>
    public Action<IServiceProvider, string>? EnterTenant { get; set; }

    /// <summary>Reads the tenant of the scope a message is sent from; null means no tenant.</summary>
    public Func<IServiceProvider, string?>? CurrentTenant { get; set; }

    public TimeSpan TenantListCacheDuration { get; set; } = TimeSpan.FromMinutes(1);
}
