using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.Storage;

namespace Twinbox.Relational;

internal static class RelationalRegistration
{
    public static TwinboxBuilder AddRelationalStore(this TwinboxBuilder builder, RelationalSettings settings)
    {
        var dialect = new RelationalDialect(settings);
        builder.Services.AddSingleton(settings);
        builder.Services.AddSingleton(dialect);
        builder.Services.TryAddSingleton<SchemaInitializer>();
        builder.Services.TryAddSingleton<RelationalOutboxStore>();
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxStore, RelationalOutboxStore>(sp => sp.GetRequiredService<RelationalOutboxStore>()));
        builder.Services.TryAddSingleton<IInboxStore, RelationalInboxStore>();
        builder.Services.TryAddSingleton<IOutboxTransactionWriter, RelationalTransactionWriter>();

        // Inserted first so the tables exist before the dispatcher or any request touches them.
        builder.Services.Insert(0, ServiceDescriptor.Singleton<IHostedService, SchemaStartupService>());
        return builder;
    }
}
