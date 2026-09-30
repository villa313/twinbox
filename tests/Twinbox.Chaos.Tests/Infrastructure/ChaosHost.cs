using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Twinbox.Chaos.Tests;

/// <summary>One application instance: its own container, dispatcher and lease identity, like a separate process.</summary>
public sealed class ChaosHost : IAsyncDisposable
{
    private readonly IHost _host;
    private readonly TestOutputLoggerProvider _logs;
    private bool _stopped;

    private ChaosHost(string instanceId, IHost host, TestOutputLoggerProvider logs)
    {
        InstanceId = instanceId;
        _host = host;
        _logs = logs;
    }

    public string InstanceId { get; }

    /// <summary>Errors this instance logged, e.g. failed dispatch passes.</summary>
    public IReadOnlyList<string> Errors => _logs.Errors;

    public IServiceProvider Services => _host.Services;

    /// <summary>True once a background service crashed the host, which the default host behavior turns into a shutdown.</summary>
    public bool Crashed => !_stopped && Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested;

    public static async Task<ChaosHost> StartAsync(
        string instanceId,
        Action<TwinboxBuilder> configure,
        Action<IServiceCollection>? services = null)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        var logs = new TestOutputLoggerProvider(instanceId);
        builder.Logging.ClearProviders().AddProvider(logs);
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(10));
        services?.Invoke(builder.Services);
        builder.Services.AddTwinbox(twinbox =>
        {
            configure(twinbox);
            twinbox.Configure(o =>
            {
                o.InstanceId = instanceId;
                o.Retention.Enabled = false;
            });
        });

        var host = builder.Build();
        await host.StartAsync();
        return new ChaosHost(instanceId, host, logs);
    }

    /// <summary>Cancels the dispatcher mid-flight; like a killed process, nothing it had leased is handed back.</summary>
    public async Task StopAsync()
    {
        if (_stopped)
        {
            return;
        }

        _stopped = true;
        await _host.StopAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _host.Dispose();
    }
}

/// <summary>Stops every instance at the end of a test, whatever state the test left them in.</summary>
public sealed class HostGroup : IAsyncDisposable
{
    private readonly List<ChaosHost> _hosts = [];

    public IReadOnlyList<ChaosHost> Hosts => _hosts;

    public ChaosHost this[int index] => _hosts[index];

    public async Task<ChaosHost> AddAsync(Task<ChaosHost> starting)
    {
        var host = await starting;
        _hosts.Add(host);
        return host;
    }

    public async ValueTask DisposeAsync() => await Task.WhenAll(_hosts.Select(h => h.DisposeAsync().AsTask()));
}

internal sealed class TestOutputLoggerProvider(string instanceId) : ILoggerProvider
{
    private readonly ConcurrentQueue<string> _errors = new();

    public IReadOnlyList<string> Errors => [.. _errors];

    public ILogger CreateLogger(string categoryName) => new TestOutputLogger(instanceId, categoryName, _errors);

    public void Dispose()
    {
    }

    private sealed class TestOutputLogger(string instanceId, string category, ConcurrentQueue<string> errors) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            var error = exception is null ? string.Empty : $" [{exception.GetType().Name}: {exception.Message}]";
            var line = $"{DateTimeOffset.UtcNow:HH:mm:ss.fff} {instanceId} {logLevel} {category}: {formatter(state, exception)}{error}";
            if (logLevel >= LogLevel.Error)
            {
                errors.Enqueue(line);
            }

            try
            {
                TestContext.Current.TestOutputHelper?.WriteLine(line);
            }
            catch (InvalidOperationException)
            {
                // Background work can outlive the test that started it; its output has nowhere to go.
            }
        }
    }
}
