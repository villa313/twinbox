using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
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
    public async Task OtherFailure_IsAbandonedAfterABackoffForItsAttempt()
    {
        var pipeline = new RecordingPipeline(_ => throw new InvalidOperationException("database down"));

        var settlement = await Handler(pipeline).HandleAsync(Received(), "orders", default);

        Assert.Equal(SettlementAction.Abandon, settlement.Action);
        Assert.Equal(TimeSpan.FromSeconds(2), settlement.Delay);
    }

    [Fact]
    public async Task FailureWhileStopping_IsAbandonedAtOnce()
    {
        using var stopping = new CancellationTokenSource();
        await stopping.CancelAsync();
        var pipeline = new RecordingPipeline(_ => throw new OperationCanceledException());

        var settlement = await Handler(pipeline).HandleAsync(Received(), "orders", stopping.Token);

        Assert.Equal(SettlementAction.Abandon, settlement.Action);
        Assert.Equal(TimeSpan.Zero, settlement.Delay);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(3, 4)]
    [InlineData(10, 30)]
    public void RetryDelay_DoublesPerDeliveryUpToItsCap(int deliveryCount, int expectedSeconds) =>
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), AzureServiceBusMessageHandler.RetryDelay(new AzureServiceBusOptions(), deliveryCount));

    private static AzureServiceBusMessageHandler Handler(IInboundPipeline pipeline) =>
        new(pipeline, Options.Create(new AzureServiceBusOptions()), NullLogger<AzureServiceBusMessageHandler>.Instance);

    private static ServiceBusReceivedMessage Received() => ServiceBusModelFactory.ServiceBusReceivedMessage(
        body: BinaryData.FromString("""{"orderId":1}"""),
        messageId: "msg-1",
        subject: "OrderPlaced",
        contentType: "application/json",
        deliveryCount: 2);
}
