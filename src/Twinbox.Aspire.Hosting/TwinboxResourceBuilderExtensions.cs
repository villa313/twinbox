using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Twinbox.Aspire.Hosting;

namespace Aspire.Hosting;

public static partial class TwinboxResourceBuilderExtensions
{
    public const string ReplayDeadLettersCommandName = "twinbox-replay-dead-letters";

    public const string OpenDashboardCommandName = "twinbox-open-dashboard";

    private const string DisplayText = "Twinbox dashboard";

    private static readonly HttpClient SharedHttpClient = new() { Timeout = TimeSpan.FromMinutes(2) };

    /// <summary>Links the Twinbox dashboard mapped at <paramref name="path"/> (via the first https, else http, endpoint unless
    /// <paramref name="endpointName"/> is given) and adds open and replay commands; replay needs a dashboard without interactive sign-in.</summary>
    public static IResourceBuilder<T> WithTwinboxDashboard<T>(this IResourceBuilder<T> builder, string path = "/twinbox", string? endpointName = null)
        where T : IResourceWithEndpoints
    {
        ArgumentNullException.ThrowIfNull(builder);
        path = NormalizePath(path);
        var resource = builder.Resource;

        builder.WithUrls(context =>
        {
            if (FindEndpoint(resource, endpointName) is { IsAllocated: true } endpoint)
            {
                context.Urls.Add(new ResourceUrlAnnotation { Url = DashboardUrl(endpoint, path).ToString(), DisplayText = DisplayText, Endpoint = endpoint });
            }
        });

        builder.WithCommand(
            ReplayDeadLettersCommandName,
            "Replay dead letters",
            context => ReplayDeadLettersAsync(context, resource, path, endpointName),
            new CommandOptions
            {
                Description = "Replays every dead outbox message through the app's Twinbox dashboard.",
                ConfirmationMessage = $"Replay every dead outbox message in {resource.Name}?",
                IconName = "ArrowRepeatAll",
                IconVariant = IconVariant.Regular,
                UpdateState = EnabledWhileRunning,
            });

        builder.WithCommand(
            OpenDashboardCommandName,
            "Open Twinbox dashboard",
            context => Task.FromResult(OpenDashboard(context, resource, path, endpointName)),
            new CommandOptions
            {
                Description = "Opens the app's Twinbox dashboard in a browser on the machine running the AppHost.",
                IconName = "Open",
                IconVariant = IconVariant.Regular,
                UpdateState = EnabledWhileRunning,
            });

        return builder;
    }

    internal static ResourceCommandState EnabledWhileRunning(UpdateCommandStateContext context) =>
        context.ResourceSnapshot.State?.Text == KnownResourceStates.Running ? ResourceCommandState.Enabled : ResourceCommandState.Disabled;

    internal static EndpointReference? FindEndpoint(IResourceWithEndpoints resource, string? endpointName)
    {
        var endpoints = resource.Annotations.OfType<EndpointAnnotation>().ToList();
        var annotation = endpointName is not null
            ? endpoints.FirstOrDefault(e => e.Name == endpointName)
            : endpoints.FirstOrDefault(e => e.UriScheme == "https") ?? endpoints.FirstOrDefault(e => e.UriScheme == "http");
        return annotation is null ? null : new EndpointReference(resource, annotation);
    }

    /// <summary>Ends with '/' so API paths resolve beneath the dashboard rather than beside it.</summary>
    internal static Uri DashboardUrl(EndpointReference endpoint, string path) => new(endpoint.Url.TrimEnd('/') + path + "/");

    private static async Task<ExecuteCommandResult> ReplayDeadLettersAsync(
        ExecuteCommandContext context,
        IResourceWithEndpoints resource,
        string path,
        string? endpointName)
    {
        if (FindEndpoint(resource, endpointName) is not { IsAllocated: true } endpoint)
        {
            return CommandResults.Failure($"{resource.Name} has no allocated http(s) endpoint to reach its Twinbox dashboard.");
        }

        var http = context.ServiceProvider.GetService<IHttpClientFactory>()?.CreateClient(TwinboxDashboardClient.HttpClientName) ?? SharedHttpClient;
        var result = await new TwinboxDashboardClient(http)
            .ReplayDeadLettersAsync(DashboardUrl(endpoint, path), context.CancellationToken)
            .ConfigureAwait(false);

        var logger = context.ServiceProvider.GetService<ResourceLoggerService>()?.GetLogger(context.ResourceName) ?? NullLogger.Instance;
        if (!result.Success)
        {
            LogReplayFailed(logger, result.Replayed, result.Error);
            return CommandResults.Failure(result.Error!);
        }

        LogReplayed(logger, result.Replayed);
        return CommandResults.Success();
    }

    private static ExecuteCommandResult OpenDashboard(ExecuteCommandContext context, IResourceWithEndpoints resource, string path, string? endpointName)
    {
        if (FindEndpoint(resource, endpointName) is not { IsAllocated: true } endpoint)
        {
            return CommandResults.Failure($"{resource.Name} has no allocated http(s) endpoint to reach its Twinbox dashboard.");
        }

        var launcher = context.ServiceProvider.GetService<BrowserLauncher>() ?? BrowserLauncher.Default;
        try
        {
            launcher.Open(DashboardUrl(endpoint, path));
            return CommandResults.Success();
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException or PlatformNotSupportedException)
        {
            return CommandResults.Failure($"Couldn't open a browser: {ex.Message}. Use the Twinbox dashboard link instead.");
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Twinbox replayed {Replayed} dead message(s).")]
    private static partial void LogReplayed(ILogger logger, int replayed);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Twinbox dead-letter replay failed after {Replayed} message(s): {Error}")]
    private static partial void LogReplayFailed(ILogger logger, int replayed, string? error);

    private static string NormalizePath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        if (!path.StartsWith('/') || path.AsSpan().IndexOfAny("{}?#") >= 0)
        {
            throw new ArgumentException("The path must be a literal path starting with '/', such as \"/twinbox\".", nameof(path));
        }

        return path.TrimEnd('/');
    }
}
