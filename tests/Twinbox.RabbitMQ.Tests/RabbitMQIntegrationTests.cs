using System.Text;
using RabbitMQ.Client;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.RabbitMQ.Tests;

[Trait("Category", "Integration")]
public sealed class RabbitMQIntegrationTests(RabbitMQFixture broker) : IClassFixture<RabbitMQFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task OutboxMessage_IsConsumedByItsHandlerOnce()
    {
        var (exchange, queue) = Names();
        var journal = new Journal();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            journal,
            b => b.Route<OrderPlaced>().To(exchange).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(queue, exchange));

        await host.SendAsync(new OrderPlaced(1));
        await journal.WaitForAsync(1, Timeout);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.Equal([1], journal.Handled);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Outbox.Snapshot()).Status);
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInbox()
    {
        var (exchange, queue) = Names();
        var journal = new Journal();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            journal,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.Listen(queue, exchange));
        var duplicate = Message("dup-1", exchange, """{"orderId":1}""");
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(duplicate, cancellation);
        await host.Transport.SendAsync(Message("after-dup", exchange, """{"orderId":2}"""), cancellation);
        await journal.WaitForAsync(2, Timeout);

        // One consumer handles the queue in order, so the sentinel arriving means both copies were processed.
        Assert.Equal([1, 2], journal.Handled);
    }

    [Fact]
    public async Task TransientHandlerFailure_IsRedeliveredWithNextAttemptNumber()
    {
        var (exchange, queue) = Names();
        var journal = new Journal();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            journal,
            b => b.AddHandler<FailFirstAttemptHandler, OrderPlaced>(),
            o => o.Listen(queue, exchange));

        await host.Transport.SendAsync(Message("retry-1", exchange, """{"orderId":5}"""), TestContext.Current.CancellationToken);

        await journal.WaitForAsync(2, Timeout);
        Assert.Equal([2], journal.Handled);
    }

    [Fact]
    public async Task TransientHandlerFailure_WaitsOutTheRetryDelayBeforeRedelivery()
    {
        var (exchange, queue) = Names();
        var journal = new Journal();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            journal,
            b => b.AddHandler<FailFirstAttemptHandler, OrderPlaced>(),
            o =>
            {
                o.Listen(queue, exchange);
                o.RetryDelay = TimeSpan.FromSeconds(2);
            });
        var started = System.Diagnostics.Stopwatch.StartNew();

        await host.Transport.SendAsync(Message("retry-2", exchange, """{"orderId":6}"""), TestContext.Current.CancellationToken);

        await journal.WaitForAsync(2, Timeout);
        Assert.True(started.Elapsed >= TimeSpan.FromSeconds(1.9), $"Redelivered after {started.Elapsed}.");
    }

    [Fact]
    public async Task TransientHandlerFailure_IsDeadLetteredOnTheLastAllowedDelivery()
    {
        var (exchange, queue) = Names();
        var journal = new Journal();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            journal,
            b => b.AddHandler<AlwaysFailingHandler, OrderPlaced>(),
            o =>
            {
                o.Listen(queue, exchange);
                o.MaxDeliveryAttempts = 3;
                o.RetryDelay = TimeSpan.FromMilliseconds(100);
            });

        await host.Transport.SendAsync(Message("exhausted-1", exchange, """{"orderId":7}"""), TestContext.Current.CancellationToken);

        var deadLettered = await WaitForMessageAsync($"{queue}.dlq");
        Assert.Equal("exhausted-1", deadLettered.BasicProperties.MessageId);
        Assert.Equal([1, 2, 3], journal.Handled);
    }

    [Fact]
    public async Task PermanentHandlerFailure_LandsInTheDeadLetterQueue()
    {
        var (exchange, queue) = Names();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            new Journal(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o => o.Listen(queue, exchange));

        await host.Transport.SendAsync(Message("poison-1", exchange, """{"orderId":3}"""), TestContext.Current.CancellationToken);

        var deadLettered = await WaitForMessageAsync($"{queue}.dlq");
        Assert.Equal("poison-1", deadLettered.BasicProperties.MessageId);
        Assert.Equal("order-placed", deadLettered.BasicProperties.Type);
    }

    [Fact]
    public async Task UnroutableMessage_IsRetryableByDefault()
    {
        var (exchange, _) = Names();
        await using var host = await RabbitMQTestHost.StartAsync(broker.ConnectionUri, new Journal(), _ => { });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => host.Transport.SendAsync(Message("lost-0", exchange, "{}"), TestContext.Current.CancellationToken));

        Assert.Contains("unroutable", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnroutableMessage_IsPermanentWhenConfigured()
    {
        var (exchange, _) = Names();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            new Journal(),
            _ => { },
            o => o.DeadLetterUnroutable = true);

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(Message("lost-1", exchange, "{}"), TestContext.Current.CancellationToken));

        Assert.Contains("unroutable", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnroutableOutboxMessage_IsDeadLetteredInTheOutbox()
    {
        var (exchange, _) = Names();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            new Journal(),
            b => b.Route<OrderPlaced>().To(exchange),
            o => o.DeadLetterUnroutable = true);

        await host.SendAsync(new OrderPlaced(4));

        using var cts = new CancellationTokenSource(Timeout);
        while (host.Outbox.Snapshot().Single().Status != OutboxMessageStatus.Dead)
        {
            await Task.Delay(50, cts.Token);
        }

        Assert.Contains("unroutable", host.Outbox.Snapshot().Single().LastError, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MissingExchange_IsReportedAsPermanentFailure()
    {
        var (exchange, _) = Names();
        await using var host = await RabbitMQTestHost.StartAsync(
            broker.ConnectionUri,
            new Journal(),
            _ => { },
            o => o.AutoProvision = false);

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(Message("lost-2", exchange, "{}"), TestContext.Current.CancellationToken));

        Assert.Contains("404", error.Message, StringComparison.Ordinal);
    }

    private static (string Exchange, string Queue) Names()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return ($"sales-{suffix}", $"orders-{suffix}");
    }

    private static TransportMessage Message(string id, string exchange, string json) => new(
        id,
        "order-placed",
        exchange,
        Encoding.UTF8.GetBytes(json),
        "application/json",
        new Dictionary<string, string>(),
        PartitionKey: null);

    private async Task<BasicGetResult> WaitForMessageAsync(string queue)
    {
        await using var connection = await broker.ConnectAsync();
        await using var channel = await connection.CreateChannelAsync();
        using var cts = new CancellationTokenSource(Timeout);
        while (true)
        {
            if (await channel.BasicGetAsync(queue, autoAck: true, cts.Token) is { } result)
            {
                return result;
            }

            await Task.Delay(50, cts.Token);
        }
    }
}
