using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Http;
using Twinbox.Transport;

namespace Twinbox;

public static class HttpTwinboxBuilderExtensions
{
    /// <summary>Sends messages as HTTP requests to the endpoint named by their destination; route with
    /// <c>To("endpoint", transport: "http")</c>. Endpoints may also come from "Twinbox:Http:Endpoints".</summary>
    public static TwinboxBuilder UseHttp(this TwinboxBuilder builder, Action<HttpOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        services.AddHttpClient();

        if (configure is not null)
        {
            // Client hooks must be registered before the container is built, so the delegate also runs once here.
            var preview = new HttpOptions();
            configure(preview);
            foreach (var (name, endpoint) in preview.Endpoints)
            {
                endpoint.ConfigureHttpClient?.Invoke(services.AddHttpClient(HttpTransport.HttpClientName(name)));
            }

            services.AddOptions<HttpOptions>().Configure(configure);
        }

        // Registered after code configuration so appsettings wins, as it does for the core options.
        services.AddOptions<HttpOptions>()
            .Configure<IServiceProvider>((options, sp) =>
            {
                if (sp.GetService<IConfiguration>() is { } configuration)
                {
                    HttpSettings.Apply(configuration, options);
                }
            })
            .ValidateOnStart();

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<HttpOptions>, HttpOptionsValidator>());
        services.TryAddSingleton(sp => new HttpTransport(
            sp.GetRequiredService<IHttpClientFactory>(),
            sp.GetRequiredService<IOptions<HttpOptions>>(),
            sp.GetRequiredService<IOptions<TwinboxOptions>>(),
            sp.GetService<TimeProvider>() ?? TimeProvider.System,
            sp.GetRequiredService<ILogger<HttpTransport>>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, HttpTransport>(sp => sp.GetRequiredService<HttpTransport>()));
        return builder;
    }
}
