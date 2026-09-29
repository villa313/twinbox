using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus.Tests;

public sealed class MessageHandlerTests
{
    [Fact]
    public async Task ProcessedMessage_IsCompleted()
    {
        var pipeline = new RecordingPipeline();

        var settlement = await Handler(pipeline).HandleAsync(Received(), "orders", default);

        Assert.Equal(SettlementAction.Complete, settlement.Action);
        var incoming = Assert.Single(pipeline.Received);
        Assert.Equal("orders", incoming.Source);
        Assert.Equal(2, incoming.DeliveryAttempt);
    }

    [Fact]
    public async Task PermanentFailure_IsDeadLetteredWithReason()
    {
        var pipeline = new RecordingPipeline(_ => throw new PermanentDeliveryException("no type for 'x'"));

        var settlement = await Handler(pipeline).HandleAsync(Received(), "orders", default);

        Assert.Equal(SettlementAction.DeadLetter, settlement.Action);
        Assert.Equal(AzureServiceBusMessageHandler.PermanentFailureReason, settlement.Reason);
        Assert.Equal("no type for 'x'", settlement.Description);
    }

    [Fact]
    public async Task OtherFailure_IsAbandonedForRedelivery()
    {
        var pipeline = new RecordingPipeline(_ => throw new InvalidOperationException("database down"));

        var settlement = await Handler(pipeline).HandleAsync(Received(), "orders", default);

        Assert.Equal(SettlementAction.Abandon, settlement.Action);
    }

    [Fact]
    public async Task Cancellation_IsAbandonedForRedelivery()
    {
        var pipeline = new RecordingPipeline(_ => throw new OperationCanceledException());

        var settlement = await Handler(pipeline).HandleAsync(Received(), "orders", default);

        Assert.Equal(SettlementAction.Abandon, settlement.Action);
    }

    private static AzureServiceBusMessageHandler Handler(IInboundPipeline pipeline) =>
        new(pipeline, NullLogger<AzureServiceBusMessageHandler>.Instance);

    private static ServiceBusReceivedMessage Received() => ServiceBusModelFactory.ServiceBusReceivedMessage(
        body: BinaryData.FromString("""{"orderId":1}"""),
        messageId: "msg-1",
        subject: "OrderPlaced",
        contentType: "application/json",
        deliveryCount: 2);
}
