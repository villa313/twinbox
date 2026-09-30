using Microsoft.Extensions.DependencyInjection;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.Dashboard;

/// <summary>The registered outbox stores, numbered in registration order, and the tenants they can be browsed for.</summary>
internal sealed class DashboardStores
{
    private readonly Lazy<IReadOnlyList<StoreEntry>> _stores;
    private readonly TenantDirectory? _tenants;

    public DashboardStores(IServiceProvider services)
    {
        _stores = new(() => [.. services.GetServices<IOutboxStore>().Select((store, i) => new StoreEntry(i, NameOf(store), store))]);
        _tenants = services.GetService<TenantDirectory>();
    }

    public IReadOnlyList<StoreEntry> All => _stores.Value;

    /// <summary>The first browsable store when <paramref name="id"/> is null.</summary>
    public StoreEntry? Find(int? id) => id is { } index
        ? All.FirstOrDefault(s => s.Id == index)
        : All.FirstOrDefault(s => s.Admin is not null);

    /// <summary>A single null entry when tenancy isn't configured.</summary>
    public Task<IReadOnlyCollection<string?>> TenantsAsync(CancellationToken cancellationToken) =>
        _tenants?.GetTenantsAsync(cancellationToken) ?? Task.FromResult<IReadOnlyCollection<string?>>([null]);

    /// <summary>Only listed tenants are entered, since entering one may pick a connection string or database.</summary>
    public async Task<(bool Known, string? Tenant)> ResolveTenantAsync(string? requested, CancellationToken cancellationToken)
    {
        var tenants = await TenantsAsync(cancellationToken).ConfigureAwait(false);
        var tenant = string.IsNullOrEmpty(requested) ? null : requested;
        if (tenant is null && tenants.Count > 0 && !tenants.Contains(null))
        {
            tenant = tenants.First();
        }

        return (tenants.Contains(tenant, StringComparer.Ordinal), tenant);
    }

    public static async Task<T> InTenantAsync<T>(string? tenant, Func<Task<T>> action)
    {
        using var _ = TenantScope.Enter(tenant);
        return await action().ConfigureAwait(false);
    }

    private static string NameOf(IOutboxStore store)
    {
        var type = store.GetType();
        var name = type.Name.Split('`')[0].Replace("OutboxStore", string.Empty, StringComparison.Ordinal);
        name = name.Length == 0 ? type.Name : name;
        return type.IsGenericType ? $"{name} ({string.Join(", ", type.GetGenericArguments().Select(a => a.Name))})" : name;
    }
}

internal sealed record StoreEntry(int Id, string Name, IOutboxStore Store)
{
    public IOutboxAdmin? Admin => Store as IOutboxAdmin;
}
