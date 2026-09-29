using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using MongoDB.Driver;
using Twinbox.MongoDB;
using Twinbox.Storage;

namespace Twinbox;

public static class MongoTwinboxBuilderExtensions
{
    /// <summary>Transactions need a replica set. Save with <c>outbox.CommitAsync(session)</c>; handlers write through <see cref="MongoHandlerSession"/>.</summary>
    public static TwinboxBuilder UseMongoDB(this TwinboxBuilder builder, string connectionString, string databaseName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);
        return builder.UseMongoDB(o =>
        {
            o.ConnectionString = connectionString;
            o.DatabaseName = databaseName;
        });
    }

    public static TwinboxBuilder UseMongoDB(this TwinboxBuilder builder, Action<MongoStorageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new MongoStorageOptions();
        configure(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.OutboxCollection, nameof(configure));
        ArgumentException.ThrowIfNullOrWhiteSpace(options.InboxCollection, nameof(configure));

        var databaseName = options.DatabaseNameFactory
            ?? (options.DatabaseName is { Length: > 0 } fixedName
                ? _ => fixedName
                : throw new ArgumentException("Set DatabaseName or DatabaseNameFactory.", nameof(configure)));

        Func<IServiceProvider, IMongoClient> client;
        if (options.ClientFactory is { } factory)
        {
            client = factory;
        }
        else if (options.ConnectionString is { Length: > 0 } connectionString)
        {
            builder.Services.TryAddSingleton(_ => new MongoClientOwner(connectionString));
            client = sp => sp.GetRequiredService<MongoClientOwner>().Client;
        }
        else
        {
            throw new ArgumentException("Set ConnectionString or ClientFactory.", nameof(configure));
        }

        var services = builder.Services;
        services.AddSingleton(new MongoSettings(client, databaseName, options.OutboxCollection, options.InboxCollection, options.CreateIndexes));
        services.TryAddSingleton<MongoIndexInitializer>();
        services.TryAddSingleton<MongoOutboxStore>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxStore, MongoOutboxStore>(sp => sp.GetRequiredService<MongoOutboxStore>()));
        services.TryAddSingleton<IInboxStore, MongoInboxStore>();
        services.TryAddScoped<MongoHandlerSession>();

        // Inserted first so the indexes exist before the dispatcher or any request touches the collections.
        services.Insert(0, ServiceDescriptor.Singleton<IHostedService, IndexStartupService>());
        return builder;
    }
}
