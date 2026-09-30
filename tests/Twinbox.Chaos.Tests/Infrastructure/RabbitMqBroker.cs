using System.Net;
using System.Net.Sockets;
using Testcontainers.RabbitMq;

namespace Twinbox.Chaos.Tests;

/// <summary>A broker of its own on a fixed port, so clients can reconnect to the same address after a restart.</summary>
public sealed class RabbitMqBroker : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4.1-alpine").WithPortBinding(Ports.Free(), 5672).Build();

    public Uri ConnectionUri => new(_container.GetConnectionString());

    public Task StopAsync() => _container.StopAsync();

    public Task StartAsync() => _container.StartAsync();

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public ValueTask DisposeAsync() => _container.DisposeAsync();
}

internal static class Ports
{
    public static int Free()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
