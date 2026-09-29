using Microsoft.Extensions.Hosting;
using Twinbox.Tenancy;

namespace Twinbox.MongoDB;

/// <summary>Creates the indexes in every tenant's database before the app starts taking requests.</summary>
internal sealed class IndexStartupService(MongoSettings settings, MongoOutboxStore store, TenantDirectory tenants) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!settings.CreateIndexes)
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
