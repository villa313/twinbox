using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Twinbox.Migration;

namespace Twinbox;

public static class MigrationTwinboxBuilderExtensions
{
    /// <summary>
    /// Keeps draining another outbox table into Twinbox while you switch over. Name your message types to match the
    /// old names with <see cref="MessageNameAttribute"/>; unknown names are skipped and logged.
    /// </summary>
    public static TwinboxBuilder ImportFromExistingOutbox(this TwinboxBuilder builder, Action<OutboxImportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new OutboxImportOptions();
        configure(options);
        if (options.CreateConnection is null || string.IsNullOrWhiteSpace(options.SelectPending) || string.IsNullOrWhiteSpace(options.MarkImported))
        {
            throw new ArgumentException("ImportFromExistingOutbox needs CreateConnection, SelectPending and MarkImported.", nameof(configure));
        }

        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton<OutboxImportService>();
        builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<OutboxImportService>());
        return builder;
    }

    /// <summary>Marks messages another system already processed as handled, so redeliveries after the switch are skipped.</summary>
    public static TwinboxBuilder SeedInboxFromExisting(this TwinboxBuilder builder, Action<InboxSeedOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new InboxSeedOptions();
        configure(options);
        if (options.CreateConnection is null || string.IsNullOrWhiteSpace(options.SelectProcessed))
        {
            throw new ArgumentException("SeedInboxFromExisting needs CreateConnection and SelectProcessed.", nameof(configure));
        }

        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton<IHostedService, InboxSeedService>();
        return builder;
    }
}
