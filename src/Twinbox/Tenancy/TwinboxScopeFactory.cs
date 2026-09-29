using Microsoft.Extensions.DependencyInjection;

namespace Twinbox.Tenancy;

/// <summary>Creates DI scopes for the tenant Twinbox is currently working for. Stores should use it instead of IServiceScopeFactory.</summary>
public sealed class TwinboxScopeFactory
{
    private readonly IServiceScopeFactory _scopes;
    private readonly TenancyOptions? _tenancy;

    public TwinboxScopeFactory(IServiceScopeFactory scopes, TenancyOptions? tenancy = null)
    {
        _scopes = scopes;
        _tenancy = tenancy;
    }

    public AsyncServiceScope CreateAsyncScope()
    {
        var scope = _scopes.CreateAsyncScope();
        if (TenantScope.Current is { } tenant && _tenancy?.EnterTenant is { } enter)
        {
            enter(scope.ServiceProvider, tenant);
        }

        return scope;
    }
}
