using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Twinbox.AzureFunctions;

namespace Twinbox;

public static class AzureFunctionsTwinboxBuilderExtensions
{
    /// <summary>Turns off background dispatch and cleanup, since Functions hosts can stop at any time; configuration can turn them back on.</summary>
    public static TwinboxBuilder UseAzureFunctions(this TwinboxBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Configure(options =>
        {
            options.Dispatcher.Enabled = false;
            options.Retention.Enabled = false;
        });
        builder.Services.TryAddSingleton<TwinboxServiceBusTrigger>();
        builder.Services.TryAddSingleton<ITwinboxMaintenance, TwinboxMaintenance>();
        return builder;
    }
}
