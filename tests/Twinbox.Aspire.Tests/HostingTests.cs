using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;
using Microsoft.Extensions.DependencyInjection;
using Twinbox.Aspire.Hosting;

namespace Twinbox.Aspire.Tests;

public sealed class HostingTests
{
    [Fact]
    public void WithTwinboxDashboard_AddsBothCommandsAndAUrlCallback()
    {
        var app = CreateApp();

        app.WithTwinboxDashboard();

        var commands = app.Resource.Annotations.OfType<ResourceCommandAnnotation>().ToList();
        Assert.Contains(commands, c => c.Name == TwinboxResourceBuilderExtensions.ReplayDeadLettersCommandName && c.DisplayName == "Replay dead letters");
        Assert.Contains(commands, c => c.Name == TwinboxResourceBuilderExtensions.OpenDashboardCommandName && c.DisplayName == "Open Twinbox dashboard");
        Assert.NotNull(commands.Single(c => c.Name == TwinboxResourceBuilderExtensions.ReplayDeadLettersCommandName).ConfirmationMessage);
        Assert.Single(app.Resource.Annotations.OfType<ResourceUrlsCallbackAnnotation>());
    }

    [Theory]
    [InlineData("Running", ResourceCommandState.Enabled)]
    [InlineData("Starting", ResourceCommandState.Disabled)]
    [InlineData("Finished", ResourceCommandState.Disabled)]
    [InlineData(null, ResourceCommandState.Disabled)]
    public void Commands_AreEnabledOnlyWhileRunning(string? state, ResourceCommandState expected)
    {
        var app = CreateApp().WithTwinboxDashboard();
        var snapshot = new CustomResourceSnapshot { ResourceType = "Project", Properties = [], State = state };

        foreach (var command in TwinboxCommands(app))
        {
            var actual = command.UpdateState(new UpdateCommandStateContext { ResourceSnapshot = snapshot, ServiceProvider = new ServiceCollection().BuildServiceProvider() });
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task UrlCallback_AddsDashboardLinkOnTheHttpsEndpoint()
    {
        var app = CreateApp().WithTwinboxDashboard("/ops/twinbox/");
        Allocate(app, "http", 5001);
        Allocate(app, "https", 7001);

        var urls = await RunUrlCallbacksAsync(app);

        var url = Assert.Single(urls);
        Assert.Equal("https://localhost:7001/ops/twinbox/", url.Url);
        Assert.Equal("Twinbox dashboard", url.DisplayText);
        Assert.Equal("https", url.Endpoint?.EndpointName);
    }

    [Fact]
    public async Task UrlCallback_UsesTheNamedEndpoint()
    {
        var app = CreateApp().WithTwinboxDashboard(endpointName: "http");
        Allocate(app, "http", 5001);
        Allocate(app, "https", 7001);

        var url = Assert.Single(await RunUrlCallbacksAsync(app));

        Assert.Equal("http://localhost:5001/twinbox/", url.Url);
    }

    [Fact]
    public async Task UrlCallback_WithoutAllocatedEndpoint_AddsNothing()
    {
        var app = CreateApp().WithTwinboxDashboard();

        Assert.Empty(await RunUrlCallbacksAsync(app));
    }

    [Theory]
    [InlineData("twinbox")]
    [InlineData("/twinbox/{id}")]
    [InlineData(" ")]
    public void WithTwinboxDashboard_RejectsNonLiteralPaths(string path)
    {
        var app = CreateApp();

        Assert.ThrowsAny<ArgumentException>(() => app.WithTwinboxDashboard(path));
    }

    [Fact]
    public async Task ReplayCommand_PostsThroughTheResourceEndpoint()
    {
        var app = CreateApp().WithTwinboxDashboard();
        Allocate(app, "https", 7001);
        var handler = FakeDashboardHandler.Healthy();

        var result = await ExecuteAsync(app, TwinboxResourceBuilderExtensions.ReplayDeadLettersCommandName, ServicesWith(handler));

        Assert.True(result.Success, result.ErrorMessage);
        var post = Assert.Single(handler.Requests, r => r.Method == HttpMethod.Post);
        Assert.Equal("https://localhost:7001/twinbox/api/dead/replay-all", post.Uri.ToString());
        Assert.Equal(FakeDashboardHandler.Token, post.Csrf);
    }

    [Fact]
    public async Task ReplayCommand_SurfacesDashboardErrors()
    {
        var app = CreateApp().WithTwinboxDashboard();
        Allocate(app, "https", 7001);
        var handler = FakeDashboardHandler.Healthy("""{"csrfToken":"t","readOnly":true}""");

        var result = await ExecuteAsync(app, TwinboxResourceBuilderExtensions.ReplayDeadLettersCommandName, ServicesWith(handler));

        Assert.False(result.Success);
        Assert.Contains("read-only", result.ErrorMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReplayCommand_WithoutAllocatedEndpoint_Fails()
    {
        var app = CreateApp().WithTwinboxDashboard();
        var handler = FakeDashboardHandler.Healthy();

        var result = await ExecuteAsync(app, TwinboxResourceBuilderExtensions.ReplayDeadLettersCommandName, ServicesWith(handler));

        Assert.False(result.Success);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task OpenCommand_OpensTheDashboardUrl()
    {
        var app = CreateApp().WithTwinboxDashboard("/ops");
        Allocate(app, "https", 7001);
        var opened = new List<Uri>();
        var services = new ServiceCollection().AddSingleton(new BrowserLauncher(opened.Add)).BuildServiceProvider();

        var result = await ExecuteAsync(app, TwinboxResourceBuilderExtensions.OpenDashboardCommandName, services);

        Assert.True(result.Success, result.ErrorMessage);
        Assert.Equal("https://localhost:7001/ops/", Assert.Single(opened).ToString());
    }

    [Fact]
    public async Task OpenCommand_BrowserFailure_ReportsIt()
    {
        var app = CreateApp().WithTwinboxDashboard();
        Allocate(app, "https", 7001);
        var services = new ServiceCollection()
            .AddSingleton(new BrowserLauncher(_ => throw new InvalidOperationException("no browser")))
            .BuildServiceProvider();

        var result = await ExecuteAsync(app, TwinboxResourceBuilderExtensions.OpenDashboardCommandName, services);

        Assert.False(result.Success);
        Assert.Contains("no browser", result.ErrorMessage, StringComparison.Ordinal);
    }

    private static IResourceBuilder<ProjectResource> CreateApp()
    {
        var builder = DistributedApplication.CreateBuilder();
        return builder.AddProject("orders", "../Orders/Orders.csproj", launchProfileName: null)
            .WithHttpEndpoint(port: 5001, name: "http")
            .WithHttpsEndpoint(port: 7001, name: "https");
    }

    private static void Allocate(IResourceBuilder<ProjectResource> app, string name, int port)
    {
        var endpoint = app.Resource.Annotations.OfType<EndpointAnnotation>().Single(e => e.Name == name);
        endpoint.AllocatedEndpoint = new AllocatedEndpoint(endpoint, "localhost", port);
    }

    private static IEnumerable<ResourceCommandAnnotation> TwinboxCommands(IResourceBuilder<ProjectResource> app) =>
        app.Resource.Annotations.OfType<ResourceCommandAnnotation>().Where(c => c.Name.StartsWith("twinbox-", StringComparison.Ordinal));

    private static async Task<List<ResourceUrlAnnotation>> RunUrlCallbacksAsync(IResourceBuilder<ProjectResource> app)
    {
        var urls = new List<ResourceUrlAnnotation>();
        var context = new ResourceUrlsCallbackContext(
            new DistributedApplicationExecutionContext(DistributedApplicationOperation.Run),
            app.Resource,
            urls,
            TestContext.Current.CancellationToken);
        foreach (var callback in app.Resource.Annotations.OfType<ResourceUrlsCallbackAnnotation>())
        {
            await callback.Callback(context);
        }

        return urls;
    }

    private static ServiceProvider ServicesWith(FakeDashboardHandler handler)
    {
        var services = new ServiceCollection();
        services.AddHttpClient(TwinboxDashboardClient.HttpClientName).ConfigurePrimaryHttpMessageHandler(() => handler);
        return services.BuildServiceProvider();
    }

    private static Task<ExecuteCommandResult> ExecuteAsync(IResourceBuilder<ProjectResource> app, string name, IServiceProvider services)
    {
        var command = app.Resource.Annotations.OfType<ResourceCommandAnnotation>().Single(c => c.Name == name);
        return command.ExecuteCommand(new ExecuteCommandContext
        {
            ServiceProvider = services,
            ResourceName = app.Resource.Name,
            CancellationToken = TestContext.Current.CancellationToken,
        });
    }
}
