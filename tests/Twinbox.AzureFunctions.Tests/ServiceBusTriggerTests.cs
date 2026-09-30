using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Twinbox.AzureServiceBus;
using Twinbox.Transport;

namespace Twinbox.AzureFunctions.Tests;

public sealed class ServiceBusTriggerTests
{
    [Fact]
    public async Task Message_IsMappedLikeTheTransport()
    {
        var pipeline = new RecordingPipeline();
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"orderId":1}"""),
            messageId: "msg-1",
            sessionId: "order-1",
            subject: "OrderPlaced",
            contentType: "application/json",
            deliveryCount: 3,
            properties: new Dictionary<string, object> { ["tenant"] = "tenant-a", ["attempt"] = 7 });

        await Trigger(pipeline).ProcessServiceBusMessageAsync(received, new RecordingMessageActions(), default);

        var incoming = Assert.Single(pipeline.Received);
        Assert.Equal("msg-1", incoming.MessageId);
        Assert.Equal("OrderPlaced", incoming.MessageName);
        Assert.Equal(TwinboxServiceBusTrigger.DefaultSource, incoming.Source);
        Assert.Equal("""{"orderId":1}""", System.Text.Encoding.UTF8.GetString(incoming.Body.Span));
        Assert.Equal("application/json", incoming.ContentType);
        Assert.Equal("tenant-a", incoming.Headers["tenant"]);
        Assert.Equal("7", incoming.Headers["attempt"]);
        Assert.Equal(3, incoming.DeliveryAttempt);
        Assert.Equal("order-1", incoming.PartitionKey);
    }

    [Fact]
    public async Task ExplicitSource_IsReportedToThePipeline()
    {
        var pipeline = new RecordingPipeline();

        await Trigger(pipeline).ProcessServiceBusMessageAsync(Received(), new RecordingMessageActions(), "billing/Subscriptions/invoices", default);

        Assert.Equal("billing/Subscriptions/invoices", Assert.Single(pipeline.Received).Source);
    }

    [Fact]
    public async Task ProcessedMessage_IsCompleted()
    {
        var actions = new RecordingMessageActions();

        await Trigger(new RecordingPipeline()).ProcessServiceBusMessageAsync(Received(), actions, default);

        Assert.Equal([new Settled("complete", "msg-1")], actions.Settlements);
    }

    [Fact]
    public async Task PermanentFailure_IsDeadLetteredWithReason()
    {
        var actions = new RecordingMessageActions();
        var pipeline = new RecordingPipeline(_ => throw new PermanentDeliveryException("no type for 'x'"));

        await Trigger(pipeline).ProcessServiceBusMessageAsync(Received(), actions, default);

        Assert.Equal(
            [new Settled("deadletter", "msg-1", AzureServiceBusInbound.PermanentFailureReason, "no type for 'x'")],
            actions.Settlements);
    }

    [Fact]
    public async Task OtherFailure_IsAbandonedForRedelivery()
    {
        var actions = new RecordingMessageActions();
        var pipeline = new RecordingPipeline(_ => throw new InvalidOperationException("database down"));

        await Trigger(pipeline).ProcessServiceBusMessageAsync(Received(), actions, default);

        Assert.Equal([new Settled("abandon", "msg-1")], actions.Settlements);
    }

    [Fact]
    public async Task CancelledInvocation_StillSettlesTheMessage()
    {
        var actions = new RecordingMessageActions();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        var pipeline = new RecordingPipeline(_ => throw new OperationCanceledException(cancelled.Token));

        await Trigger(pipeline).ProcessServiceBusMessageAsync(Received(), actions, cancelled.Token);

        Assert.Equal([new Settled("abandon", "msg-1")], actions.Settlements);
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInboxAndStillCompleted()
    {
        var journal = new Journal();
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(journal)
            .AddTwinbox(b => b.UseInMemoryStore().UseAzureFunctions().AddHandler<OrderPlacedHandler, OrderPlaced>())
            .BuildServiceProvider();
        var trigger = services.GetRequiredService<TwinboxServiceBusTrigger>();
        var actions = new RecordingMessageActions();

        await trigger.ProcessServiceBusMessageAsync(Received(deliveryCount: 1), actions, default);
        await trigger.ProcessServiceBusMessageAsync(Received(deliveryCount: 2), actions, default);

        Assert.Equal(["msg-1:1"], journal.Entries);
        Assert.Equal([new Settled("complete", "msg-1"), new Settled("complete", "msg-1")], actions.Settlements);
    }

    [Fact]
    public async Task UnknownMessage_IsDeadLetteredByDefault()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseAzureFunctions())
            .BuildServiceProvider();
        var actions = new RecordingMessageActions();

        await services.GetRequiredService<TwinboxServiceBusTrigger>().ProcessServiceBusMessageAsync(Received(), actions, default);

        Assert.Equal("deadletter", Assert.Single(actions.Settlements).Action);
    }

    private static TwinboxServiceBusTrigger Trigger(IInboundPipeline pipeline) =>
        new(pipeline, NullLogger<TwinboxServiceBusTrigger>.Instance);

    private static ServiceBusReceivedMessage Received(int deliveryCount = 1) => ServiceBusModelFactory.ServiceBusReceivedMessage(
        body: BinaryData.FromString("""{"orderId":1}"""),
        messageId: "msg-1",
        subject: "OrderPlaced",
        contentType: "application/json",
        deliveryCount: deliveryCount);
}
