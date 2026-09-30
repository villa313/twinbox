using System.Diagnostics.CodeAnalysis;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.AzureServiceBus;
using Twinbox.Transport;

namespace Twinbox;

public static class AzureServiceBusTwinboxBuilderExtensions
{
    // The processor's default MaxAutoLockRenewalDuration; a longer hold would lose the lock before the abandon.
    private static readonly TimeSpan MaxLockRenewal = TimeSpan.FromMinutes(5);

    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UseAzureServiceBus(
        this TwinboxBuilder builder,
        string connectionString,
        Action<AzureServiceBusOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return builder.UseAzureServiceBus(_ => new ServiceBusClient(connectionString), configure);
    }

    /// <summary>For token credentials such as DefaultAzureCredential; Twinbox disposes the returned client on shutdown.</summary>
    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UseAzureServiceBus(
        this TwinboxBuilder builder,
        Func<IServiceProvider, ServiceBusClient> clientFactory,
        Action<AzureServiceBusOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(clientFactory);

        var options = builder.Services.AddOptions<AzureServiceBusOptions>()
            .Validate(o => o.MaxConcurrentCalls > 0, "AzureServiceBusOptions.MaxConcurrentCalls must be positive.")
            .Validate(o => o.PrefetchCount >= 0, "AzureServiceBusOptions.PrefetchCount cannot be negative.")
            .Validate(o => o.RetryDelay > TimeSpan.Zero, "AzureServiceBusOptions.RetryDelay must be positive.")
            .Validate(o => o.MaxRetryDelay >= o.RetryDelay && o.MaxRetryDelay <= MaxLockRenewal, "AzureServiceBusOptions.MaxRetryDelay must be between RetryDelay and 5 minutes.");
        if (configure is not null)
        {
            options.Configure(configure);
        }

        builder.Services.TryAddSingleton(sp => new AzureServiceBusTransport(
            clientFactory(sp), sp.GetRequiredService<IOptions<AzureServiceBusOptions>>().Value));
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ITransport, AzureServiceBusTransport>(sp => sp.GetRequiredService<AzureServiceBusTransport>()));
        builder.Services.TryAddSingleton<AzureServiceBusMessageHandler>();
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, AzureServiceBusReceiverService>());
        return builder;
    }
}
