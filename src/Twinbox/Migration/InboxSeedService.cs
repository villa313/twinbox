using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Twinbox.Storage;
using Twinbox.Tenancy;

namespace Twinbox.Migration;

/// <summary>Runs once at startup; entries already present are left alone, so restarting is harmless.</summary>
internal sealed partial class InboxSeedService(
    InboxSeedOptions options,
    IInboxStore inbox,
    TenantDirectory tenants,
    TwinboxScopeFactory scopes,
    TimeProvider time,
    ILogger<InboxSeedService> logger) : IHostedService
{
    private readonly ILogger _logger = logger;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var tenant in await tenants.GetTenantsAsync(cancellationToken).ConfigureAwait(false))
        {
            using var _ = TenantScope.Enter(tenant);
            LogSeeded(await SeedTenantAsync(cancellationToken).ConfigureAwait(false), tenant);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task<int> SeedTenantAsync(CancellationToken cancellationToken)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            await using var connection = options.CreateConnection!(scope.ServiceProvider);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = options.SelectProcessed;

            // Read everything first: the old tables often share the database, and an open reader can block the inserts.
            var entries = new List<InboxEntry>();
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    entries.Add(new InboxEntry(
                        Convert.ToString(reader.GetValue(reader.GetOrdinal("MessageId")), CultureInfo.InvariantCulture)!,
                        reader.GetString(reader.GetOrdinal("Consumer")),
                        "import",
                        time.GetUtcNow()));
                }
            }

            await connection.CloseAsync().ConfigureAwait(false);

            var seeded = 0;
            foreach (var entry in entries)
            {
                var entryScope = scopes.CreateAsyncScope();
                await using (entryScope.ConfigureAwait(false))
                {
                    if (await inbox.TryProcessAsync(entry, entryScope.ServiceProvider, _ => Task.CompletedTask, cancellationToken).ConfigureAwait(false))
                    {
                        seeded++;
                    }
                }
            }

            return seeded;
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Seeded {Count} processed message(s) into the inbox (tenant {Tenant}).")]
    private partial void LogSeeded(int count, string? tenant);
}
