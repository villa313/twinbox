using Microsoft.Extensions.Hosting;
using Twinbox.Tenancy;

namespace Twinbox.Relational;

/// <summary>Creates the tables in every tenant's database before the app starts taking requests.</summary>
internal sealed class SchemaStartupService(RelationalSettings settings, RelationalOutboxStore store, TenantDirectory tenants) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!settings.CreateSchemaIfMissing)
        {
            return;
        }

        await tenants.ForEachTenantAsync(
            async ct =>
            {
                await using (await store.OpenAsync(ct).ConfigureAwait(false))
                {
                }
            },
            cancellationToken).ConfigureAwait(false);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
