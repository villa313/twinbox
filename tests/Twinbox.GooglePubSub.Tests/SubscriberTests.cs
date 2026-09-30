using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Twinbox.Tests.Shared;
using Twinbox.Transport;
using Reply = Google.Cloud.PubSub.V1.SubscriberClient.Reply;

namespace Twinbox.GooglePubSub.Tests;

public sealed class SubscriberTests
{
    [Fact]
    public async Task HandledMessage_IsAcknowledged()
    {
        var pipeline = new ScriptedPipeline();

        var reply = await Service(pipeline).HandleAsync("billing", Received("m1"), Gate(), CancellationToken.None);

        Assert.Equal(Reply.Ack, reply);
        var incoming = Assert.Single(pipeline.Seen);
        Assert.Equal("billing", incoming.Source);
        Assert.Equal(1, incoming.DeliveryAttempt);
    }

    [Fact]
    public async Task TransientFailure_IsNacked()
    {
        var pipeline = new ScriptedPipeline { Failure = new InvalidOperationException("database down") };

        Assert.Equal(Reply.Nack, await Service(pipeline).HandleAsync("billing", Received("m1"), Gate(), CancellationToken.None));
    }

    [Fact]
    public async Task PermanentFailureOnASubscriptionWithADeadLetterPolicy_IsNackedForThePolicy()
    {
        var pipeline = new ScriptedPipeline { Failure = new PermanentDeliveryException("poison") };
        var received = Received("m1");
        received.Attributes["googclient_deliveryattempt"] = "2";

        Assert.Equal(Reply.Nack, await Service(pipeline).HandleAsync("billing", received, Gate(), CancellationToken.None));
    }

    [Fact]
    public async Task PermanentFailureWithNowhereToDeadLetter_IsCountedAndAcknowledged()
    {
        var subscription = $"billing-{Guid.NewGuid():N}";
        using var discarded = new CounterProbe("twinbox.inbox.discarded", subscription);
        var pipeline = new ScriptedPipeline { Failure = new PermanentDeliveryException("poison") };

        Assert.Equal(Reply.Ack, await Service(pipeline).HandleAsync(subscription, Received("m1"), Gate(), CancellationToken.None));
        Assert.Equal(1, discarded.Value);
    }

    [Fact]
    public async Task AfterAFailure_LaterMessagesOfTheKeyWaitForItsRedelivery()
    {
        var pipeline = new ScriptedPipeline { Failure = new InvalidOperationException("database down") };
        var service = Service(pipeline);
        var gate = Gate();

        Assert.Equal(Reply.Nack, await service.HandleAsync("billing", Received("m1", "customer-1", 1), gate, CancellationToken.None));
        pipeline.Failure = null;

        Assert.Equal(Reply.Nack, await service.HandleAsync("billing", Received("m2", "customer-1", 2), gate, CancellationToken.None));
        Assert.Equal(Reply.Ack, await service.HandleAsync("billing", Received("m9", "customer-2", 3), gate, CancellationToken.None));
        Assert.Equal(Reply.Ack, await service.HandleAsync("billing", Received("m1", "customer-1", 1), gate, CancellationToken.None));
        Assert.Equal(Reply.Ack, await service.HandleAsync("billing", Received("m2", "customer-1", 2), gate, CancellationToken.None));

        Assert.Equal(["m9", "m1", "m2"], pipeline.Seen.Select(m => m.MessageId));
    }

    [Fact]
    public void Gate_ReleasesSkippedMessagesInPublishOrderWhateverOrderTheyReturnIn()
    {
        var gate = Gate();
        gate.Fail("customer-1", "m1", At(1));

        Assert.False(gate.TryEnter("customer-1", "m2", At(2)));
        Assert.True(gate.TryEnter("customer-1", "m1", At(1)));
        Assert.False(gate.TryEnter("customer-1", "m3", At(3)));
        Assert.True(gate.TryEnter("customer-1", "m2", At(2)));
        Assert.True(gate.TryEnter("customer-1", "m3", At(3)));
        Assert.True(gate.TryEnter("customer-1", "m4", At(4)));
    }

    [Fact]
    public void Gate_OrdersMessagesPublishedTogetherByArrival()
    {
        var gate = Gate();
        gate.Fail("customer-1", "m1", At(1));

        Assert.False(gate.TryEnter("customer-1", "m2", At(1)));
        Assert.False(gate.TryEnter("customer-1", "m2", At(1)));
        Assert.True(gate.TryEnter("customer-1", "m1", At(1)));
        Assert.True(gate.TryEnter("customer-1", "m2", At(1)));
    }

    [Fact]
    public void Gate_KeepsOtherKeysFlowing()
    {
        var gate = Gate();
        gate.Fail("customer-1", "m1", At(1));

        Assert.True(gate.TryEnter("customer-2", "m2", At(2)));
    }

    [Fact]
    public void Gate_StopsWaitingForAMessageThatNeverReturns()
    {
        var time = new FakeTimeProvider();
        var gate = new OrderingKeyGate(TimeSpan.FromMinutes(1), time);
        gate.Fail("customer-1", "m1", At(1));

        Assert.False(gate.TryEnter("customer-1", "m2", At(2)));
        time.Advance(TimeSpan.FromMinutes(2));

        Assert.True(gate.TryEnter("customer-1", "m2", At(2)));
    }

    private static GooglePubSubSubscriberService Service(IInboundPipeline pipeline) => new(
        RegistrationTests.Clients(new GooglePubSubOptions { ProjectId = "p1" }),
        pipeline,
        NullLogger<GooglePubSubSubscriberService>.Instance);

    private static OrderingKeyGate Gate() => new(TimeSpan.FromMinutes(10), TimeProvider.System);

    private static DateTimeOffset At(int second) => DateTimeOffset.UnixEpoch.AddSeconds(second);

    private static PubsubMessage Received(string id, string orderingKey = "", int publishedAt = 0) => new()
    {
        MessageId = id,
        OrderingKey = orderingKey,
        PublishTime = Timestamp.FromDateTimeOffset(At(publishedAt)),
    };

    private sealed class ScriptedPipeline : IInboundPipeline
    {
        public Exception? Failure { get; set; }

        public List<IncomingMessage> Seen { get; } = [];

        public Task ProcessAsync(IncomingMessage message, CancellationToken cancellationToken)
        {
            if (Failure is { } failure)
            {
                return Task.FromException(failure);
            }

            Seen.Add(message);
            return Task.CompletedTask;
        }
    }
}
