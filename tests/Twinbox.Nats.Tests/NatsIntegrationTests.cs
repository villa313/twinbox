using System.Text;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Twinbox.Storage;
using Twinbox.Transport;

namespace Twinbox.Nats.Tests;

[Trait("Category", "Integration")]
public sealed class NatsIntegrationTests(NatsFixture server) : IClassFixture<NatsFixture>
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task OutboxMessage_IsConsumedByItsHandlerOnce()
    {
        var names = Names.Create();
        var journal = new Journal();
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            journal,
            new FailureGate(),
            b => b.Route<OrderPlaced>().To(names.Subject("placed")).AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.AddStream(names.Stream, names.AllSubjects).Listen(names.Stream, names.Consumer));

        await host.SendAsync(new OrderPlaced(1));
        await journal.WaitForAsync(1, Timeout);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        var entry = Assert.Single(journal.Entries);
        Assert.Equal(names.Subject("placed"), entry.Source);
        Assert.Equal(1, entry.DeliveryAttempt);
        Assert.Equal(OutboxMessageStatus.Sent, Assert.Single(host.Outbox.Snapshot()).Status);
    }

    [Fact]
    public async Task RepeatedSend_IsDroppedByTheServerThroughNatsMsgId()
    {
        var names = Names.Create();
        var journal = new Journal();
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.AddStream(names.Stream, names.AllSubjects).Listen(names.Stream, names.Consumer));
        var cancellation = TestContext.Current.CancellationToken;
        var message = Message("dup-1", names.Subject("placed"), 1, "customer-1");

        await host.Transport.SendAsync(message, cancellation);
        await host.Transport.SendAsync(message, cancellation);
        await host.Transport.SendAsync(Message("after-dup", names.Subject("placed"), 2, "customer-1"), cancellation);
        await journal.WaitForAsync(2, Timeout);

        Assert.Equal(2, await server.StoredMessagesAsync(names.Stream));
        Assert.Equal([1, 2], journal.Handled);
    }

    [Fact]
    public async Task DuplicateDelivery_IsSkippedByTheInbox()
    {
        var names = Names.Create();
        var journal = new Journal();
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.AddStream(names.Stream, names.AllSubjects).Listen(names.Stream, names.Consumer));
        var duplicate = Message("dup-2", names.Subject("placed"), 1, "customer-1");

        // Distinct Nats-Msg-Ids get both copies past the server, as a republish after the duplicate window would.
        await PublishRawAsync(duplicate, "first-copy");
        await PublishRawAsync(duplicate, "second-copy");
        await PublishRawAsync(Message("after-dup-2", names.Subject("placed"), 2, "customer-1"), "sentinel");
        await journal.WaitForAsync(2, Timeout);

        Assert.Equal(3, await server.StoredMessagesAsync(names.Stream));
        Assert.Equal([1, 2], journal.Handled);
    }

    [Fact]
    public async Task WildcardFilterSubjects_OnlyDeliverMatchingSubjects()
    {
        var names = Names.Create();
        var journal = new Journal();
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            journal,
            new FailureGate(),
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o
                .AddStream(names.Stream, names.AllSubjects)
                .Listen(names.Stream, $"{names.Consumer}-orders", names.Subject("orders.*"))
                .Listen(names.Stream, $"{names.Consumer}-audit", names.Subject("audit.>")));
        var cancellation = TestContext.Current.CancellationToken;

        await host.Transport.SendAsync(Message("w-1", names.Subject("orders.placed"), 1, null), cancellation);
        await host.Transport.SendAsync(Message("w-2", names.Subject("orders.eu.placed"), 2, null), cancellation);
        await host.Transport.SendAsync(Message("w-3", names.Subject("audit.eu.placed"), 3, null), cancellation);
        await host.Transport.SendAsync(Message("w-4", names.Subject("other.placed"), 4, null), cancellation);
        await journal.WaitForAsync(1, Timeout);
        await journal.WaitForAsync(3, Timeout);
        await Task.Delay(1000, cancellation);

        Assert.Equal([1, 3], journal.Handled.Order());
        Assert.Contains(journal.Entries, e => e.OrderId == 3 && e.Source == names.Subject("audit.eu.placed"));
    }

    [Fact]
    public async Task PermanentHandlerFailure_IsTerminatedWithoutRedelivery()
    {
        var names = Names.Create();
        var journal = new Journal();
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            journal,
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o =>
            {
                o.AckWait = TimeSpan.FromSeconds(1);
                o.AddStream(names.Stream, names.AllSubjects).Listen(names.Stream, names.Consumer);
            });

        await host.Transport.SendAsync(Message("poison-1", names.Subject("placed"), 3, null), TestContext.Current.CancellationToken);
        await journal.WaitForAsync(3, Timeout);
        await WaitForSettledAsync(names);
        await Task.Delay(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);

        Assert.Single(journal.Entries);
        Assert.Equal(0, (await server.ConsumerInfoAsync(names.Stream, names.Consumer)).NumAckPending);
    }

    [Fact]
    public async Task PermanentHandlerFailure_IsCopiedToTheDeadLetterSubjectAndTerminated()
    {
        var names = Names.Create();
        var deadLetterStream = $"{names.Stream}_DLQ";
        var deadLetterSubject = $"dead-{names.Suffix}.orders";
        var journal = new Journal();
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            journal,
            new FailureGate(),
            b => b.AddHandler<RejectingHandler, OrderPlaced>(),
            o =>
            {
                o.DeadLetterSubject = deadLetterSubject;
                o.AddStream(names.Stream, names.AllSubjects)
                    .AddStream(deadLetterStream, deadLetterSubject)
                    .Listen(names.Stream, names.Consumer);
            });

        await host.Transport.SendAsync(Message("poison-2", names.Subject("placed"), 3, "customer-3"), TestContext.Current.CancellationToken);

        var copy = await server.ReadFirstAsync(deadLetterStream, Timeout);
        Assert.Equal("poison-2", Header(copy, TransportHeaders.MessageId));
        Assert.Equal("order-placed", Header(copy, TransportHeaders.MessageName));
        Assert.Equal("customer-3", Header(copy, TransportHeaders.PartitionKey));
        Assert.Contains("can never be handled", Header(copy, NatsMapping.ErrorHeader), StringComparison.Ordinal);
        Assert.Equal($"{names.Stream}:1", Header(copy, NatsMapping.OriginHeader));
        Assert.Equal("""{"orderId":3}""", Encoding.UTF8.GetString(copy.Data!));
        await WaitForSettledAsync(names);
        Assert.Single(journal.Entries);
    }

    [Fact]
    public async Task TransientHandlerFailure_IsRedeliveredWithTheNextAttempt()
    {
        var names = Names.Create();
        var journal = new Journal();
        var gate = new FailureGate();
        gate.Hold(5);
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            journal,
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o => o.AddStream(names.Stream, names.AllSubjects).Listen(names.Stream, names.Consumer));

        await host.Transport.SendAsync(Message("flaky-5", names.Subject("placed"), 5, null), TestContext.Current.CancellationToken);
        await gate.WaitForFailuresAsync(1, Timeout);
        gate.Release(5);
        await journal.WaitForAsync(5, Timeout);

        var entry = Assert.Single(journal.Entries);
        Assert.Equal(2, entry.DeliveryAttempt);
        await WaitForSettledAsync(names);
    }

    [Fact]
    public async Task HandlerFailingOnEveryDelivery_IsDeadLetteredOnTheLastOne()
    {
        var names = Names.Create();
        var deadLetterStream = $"{names.Stream}_DLQ";
        var deadLetterSubject = $"dead-{names.Suffix}.orders";
        var gate = new FailureGate();
        gate.Hold(6);
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            new Journal(),
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.MaxDeliveryAttempts = 3;
                o.DeadLetterSubject = deadLetterSubject;
                o.AddStream(names.Stream, names.AllSubjects)
                    .AddStream(deadLetterStream, deadLetterSubject)
                    .Listen(names.Stream, names.Consumer);
            });

        await host.Transport.SendAsync(Message("doomed-6", names.Subject("placed"), 6, null), TestContext.Current.CancellationToken);

        var copy = await server.ReadFirstAsync(deadLetterStream, Timeout);
        Assert.Equal("doomed-6", Header(copy, TransportHeaders.MessageId));
        Assert.Contains("is held", Header(copy, NatsMapping.ErrorHeader), StringComparison.Ordinal);
        Assert.Equal(3, gate.Failures);
    }

    [Fact]
    public async Task DeadLetterCopyFailingOnTheLastDelivery_IsRetriedUntilItLands()
    {
        var names = Names.Create();
        var deadLetterStream = $"{names.Stream}_DLQ";
        var deadLetterSubject = $"dead-{names.Suffix}.orders";
        var gate = new FailureGate();
        gate.Hold(9);
        await using var host = await NatsTestHost.StartAsync(
            server.Url,
            new Journal(),
            gate,
            b => b.AddHandler<RecordingHandler, OrderPlaced>(),
            o =>
            {
                o.MaxDeliveryAttempts = 2;
                o.DeadLetterSubject = deadLetterSubject;
                o.AddStream(names.Stream, names.AllSubjects).Listen(names.Stream, names.Consumer);
            });

        await host.Transport.SendAsync(Message("stranded-9", names.Subject("placed"), 9, null), TestContext.Current.CancellationToken);
        await gate.WaitForFailuresAsync(2, Timeout);
        await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        await server.JetStream.CreateStreamAsync(new StreamConfig(deadLetterStream, [deadLetterSubject]), TestContext.Current.CancellationToken);

        var copy = await server.ReadFirstAsync(deadLetterStream, Timeout);
        Assert.Equal("stranded-9", Header(copy, TransportHeaders.MessageId));
        Assert.Equal(2, gate.Failures);
        await WaitForSettledAsync(names);
    }

    [Fact]
    public async Task SendToSubjectWithoutStream_IsTransientUnlessUnroutableIsDeadLettered()
    {
        var names = Names.Create();
        var message = Message("nowhere-1", $"nowhere-{names.Suffix}.placed", 7, null);
        await using (var host = await NatsTestHost.StartAsync(server.Url, new Journal(), new FailureGate(), _ => { }))
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(
                () => host.Transport.SendAsync(message, TestContext.Current.CancellationToken));
            Assert.Contains(message.Destination, error.Message, StringComparison.Ordinal);
        }

        await using (var host = await NatsTestHost.StartAsync(
            server.Url, new Journal(), new FailureGate(), _ => { }, o => o.DeadLetterUnroutable = true))
        {
            await Assert.ThrowsAsync<PermanentDeliveryException>(
                () => host.Transport.SendAsync(message, TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    public async Task OversizedMessage_IsPermanentFailure()
    {
        var names = Names.Create();
        await using var host = await NatsTestHost.StartAsync(
            server.Url, new Journal(), new FailureGate(), _ => { }, o => o.AddStream(names.Stream, names.AllSubjects));
        var oversized = Message("huge-1", names.Subject("placed"), 8, null) with { Body = new byte[2 * 1024 * 1024] };

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(
            () => host.Transport.SendAsync(oversized, TestContext.Current.CancellationToken));

        Assert.Contains(names.Subject("placed"), error.Message, StringComparison.Ordinal);
    }

    private static TransportMessage Message(string id, string subject, int orderId, string? partitionKey) => new(
        id,
        "order-placed",
        subject,
        Encoding.UTF8.GetBytes($$"""{"orderId":{{orderId}}}"""),
        "application/json",
        new Dictionary<string, string>(),
        partitionKey);

    private static string Header(NatsJSMsg<byte[]> msg, string name) => msg.Headers![name].ToString();

    private async Task PublishRawAsync(TransportMessage message, string natsMsgId)
    {
        var headers = NatsMapping.ToHeaders(message);
        headers[NatsMapping.MsgIdHeader] = natsMsgId;
        var ack = await server.JetStream.PublishAsync(
            message.Destination,
            message.Body,
            NatsRawSerializer<ReadOnlyMemory<byte>>.Default,
            headers: headers,
            cancellationToken: TestContext.Current.CancellationToken);
        ack.EnsureSuccess();
    }

    private async Task WaitForSettledAsync(Names names)
    {
        using var cts = new CancellationTokenSource(Timeout);
        while (true)
        {
            var info = await server.ConsumerInfoAsync(names.Stream, names.Consumer);
            if (info.NumAckPending == 0 && info.NumPending == 0 && info.AckFloor.StreamSeq > 0)
            {
                return;
            }

            await Task.Delay(100, cts.Token);
        }
    }

    private sealed record Names(string Suffix)
    {
        public string Stream => $"ORDERS_{Suffix}";

        public string Consumer => $"billing-{Suffix}";

        public string AllSubjects => $"orders-{Suffix}.>";

        public static Names Create() => new(Guid.NewGuid().ToString("N")[..8]);

        public string Subject(string tail) => $"orders-{Suffix}.{tail}";
    }
}
