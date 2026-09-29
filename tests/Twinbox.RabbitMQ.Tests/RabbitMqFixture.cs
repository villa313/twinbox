using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace Twinbox.RabbitMQ.Tests;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4.1-alpine").Build();

    public Uri ConnectionUri => new(_container.GetConnectionString());

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public ValueTask DisposeAsync() => _container.DisposeAsync();

    /// <summary>A plain connection for inspecting the broker from tests.</summary>
    public Task<IConnection> ConnectAsync() =>
        new ConnectionFactory { Uri = ConnectionUri }.CreateConnectionAsync();
}
