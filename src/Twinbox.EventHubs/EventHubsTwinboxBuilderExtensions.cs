using Azure.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.EventHubs;
using Twinbox.Transport;

namespace Twinbox;

public static class EventHubsTwinboxBuilderExtensions
{
    public static TwinboxBuilder UseEventHubs(this TwinboxBuilder builder, string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return builder.UseEventHubs(options => options.ConnectionString = connectionString);
    }

    /// <summary>For token credentials such as DefaultAzureCredential; use the options overload to also listen.</summary>
    public static TwinboxBuilder UseEventHubs(this TwinboxBuilder builder, string fullyQualifiedNamespace, TokenCredential credential)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fullyQualifiedNamespace);
        ArgumentNullException.ThrowIfNull(credential);
        return builder.UseEventHubs(options =>
        {
            options.FullyQualifiedNamespace = fullyQualifiedNamespace;
            options.Credential = credential;
        });
    }

    /// <summary>Sends through Event Hubs and consumes the event hubs registered with <see cref="EventHubsOptions.Listen(string, string, Uri)"/>.</summary>
    public static TwinboxBuilder UseEventHubs(this TwinboxBuilder builder, Action<EventHubsOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<EventHubsOptions>()
            .Configure(configure)
            .Validate(
                o => !string.IsNullOrWhiteSpace(o.ConnectionString) || (!string.IsNullOrWhiteSpace(o.FullyQualifiedNamespace) && o.Credential is not null),
                "Event Hubs needs a ConnectionString, or a FullyQualifiedNamespace with a Credential.")
            .Validate(o => o.RetryDelay > TimeSpan.Zero, "Event Hubs RetryDelay must be positive.")
            .Validate(o => o.MaxRetryDelay >= o.RetryDelay, "Event Hubs MaxRetryDelay cannot be shorter than RetryDelay.")
            .Validate(o => o.MaxBatchSize > 0, "Event Hubs MaxBatchSize must be positive.")
            .Validate(o => o.DeadLetterEventHub is null || !string.IsNullOrWhiteSpace(o.DeadLetterEventHub), "Event Hubs DeadLetterEventHub cannot be blank.");

        builder.Services.TryAddSingleton<EventHubsClients>();
        builder.Services.TryAddSingleton<EventHubsEventHandler>();
        builder.Services.TryAddSingleton(sp => new EventHubsTransport(sp.GetRequiredService<EventHubsClients>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, EventHubsTransport>(sp => sp.GetRequiredService<EventHubsTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, EventHubsProcessorService>());
        return builder;
    }
}
