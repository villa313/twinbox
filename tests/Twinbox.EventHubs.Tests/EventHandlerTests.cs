using Azure.Messaging.EventHubs;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.EventHubs.Tests;

public sealed class EventHandlerTests
{
    private readonly FakePipeline _pipeline = new();
    private readonly List<long> _checkpoints = [];

    [Fact]
    public async Task SingleEvent_IsProcessedAndCheckpointed()
    {
        await Handler().HandleAsync(Partition(), Events(1), CancellationToken.None);

        Assert.Equal(["msg-1@1"], _pipeline.Calls);
        Assert.Equal([1L], _checkpoints);
    }

    [Fact]
    public async Task EmptyRead_DoesNothing()
    {
        await Handler().HandleAsync(Partition(), [], CancellationToken.None);

        Assert.Empty(_pipeline.Calls);
        Assert.Empty(_checkpoints);
    }

    [Fact]
    public async Task SuccessfulBatch_IsCheckpointedOnceAtItsLastEvent()
    {
        await Handler().HandleAsync(Partition(), Events(1, 2, 3), CancellationToken.None);

        Assert.Equal(["batch[msg-1,msg-2,msg-3]"], _pipeline.Calls);
        Assert.Equal([3L], _checkpoints);
    }

    [Fact]
    public async Task FailedBatch_FallsBackToOneByOneAndSkipsThePermanentFailure()
    {
        _pipeline.FailBatches = true;
        _pipeline.Permanent.Add("msg-2");

        await Handler().HandleAsync(Partition(), Events(1, 2, 3), CancellationToken.None);

        Assert.Equal(["batch[msg-1,msg-2,msg-3]", "msg-1@1", "msg-2@1", "msg-3@1"], _pipeline.Calls);
        Assert.Equal([1L, 2L, 3L], _checkpoints);
    }

    [Fact]
    public async Task TransientFailure_IsRetriedInPlaceWithACountedAttempt()
    {
        _pipeline.TransientFailures["msg-1"] = 2;

        await Handler().HandleAsync(Partition(), Events(1, 2), CancellationToken.None);

        Assert.Equal(["batch[msg-1,msg-2]", "msg-1@1", "msg-1@2", "msg-1@3", "msg-2@1"], _pipeline.Calls);
        Assert.Equal([1L, 2L], _checkpoints);
    }

    [Fact]
    public async Task StoppingWhileRetrying_LeavesTheEventUncheckpointed()
    {
        _pipeline.TransientFailures["msg-1"] = int.MaxValue;
        using var stopping = new CancellationTokenSource();
        var handling = Handler().HandleAsync(Partition(), Events(1, 2), stopping.Token);

        await WaitUntilAsync(() => _pipeline.Calls.Count >= 3);
        await stopping.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => handling);
        Assert.Empty(_checkpoints);
        Assert.DoesNotContain("msg-2@1", _pipeline.Calls);
    }

    [Fact]
    public async Task FailedCheckpoint_DoesNotStopTheNextEvent()
    {
        var failingCheckpoints = new PartitionContext("orders", "0", (_, _) => throw new InvalidOperationException("storage down"));

        await Handler().HandleAsync(failingCheckpoints, Events(1), CancellationToken.None);
        await Handler().HandleAsync(failingCheckpoints, Events(2), CancellationToken.None);

        Assert.Equal(["msg-1@1", "msg-2@1"], _pipeline.Calls);
    }

    [Theory]
    [InlineData(1, 100)]
    [InlineData(2, 200)]
    [InlineData(4, 800)]
    [InlineData(5, 1000)]
    [InlineData(500, 1000)]
    public void RetryDelay_DoublesUpToItsCap(int attempt, int expectedMilliseconds)
    {
        var options = new EventHubsOptions { RetryDelay = TimeSpan.FromMilliseconds(100), MaxRetryDelay = TimeSpan.FromSeconds(1) };

        Assert.Equal(TimeSpan.FromMilliseconds(expectedMilliseconds), EventHubsEventHandler.RetryDelay(options, attempt));
    }

    private static List<EventData> Events(params int[] sequenceNumbers) =>
        [.. sequenceNumbers.Select(n => MappingTests.Received(
            new Dictionary<string, object> { [TransportHeaders.MessageId] = $"msg-{n}", [TransportHeaders.MessageName] = "order-placed" },
            sequenceNumber: n))];

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!condition())
        {
            await Task.Delay(10, cts.Token);
        }
    }

    private EventHubsEventHandler Handler()
    {
        var options = new EventHubsOptions
        {
            ConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=key;SharedAccessKey=secret",
            RetryDelay = TimeSpan.FromMilliseconds(1),
            MaxRetryDelay = TimeSpan.FromMilliseconds(5),
        };
        return new EventHubsEventHandler(new EventHubsClients(Options.Create(options)), _pipeline, NullLogger<EventHubsEventHandler>.Instance);
    }

    private PartitionContext Partition() => new("orders", "0", (data, _) =>
    {
        lock (_checkpoints)
        {
            _checkpoints.Add(data.SequenceNumber);
        }

        return Task.CompletedTask;
    });

    private sealed class FakePipeline : IInboundPipeline
    {
        private readonly List<string> _calls = [];

        public bool FailBatches { get; set; }

        public HashSet<string> Permanent { get; } = [];

        public Dictionary<string, int> TransientFailures { get; } = [];

        public IReadOnlyList<string> Calls
        {
            get
            {
                lock (_calls)
                {
                    return [.. _calls];
                }
            }
        }

        public Task ProcessAsync(IncomingMessage message, CancellationToken cancellationToken)
        {
            Record($"{message.MessageId}@{message.DeliveryAttempt}");
            if (Permanent.Contains(message.MessageId))
            {
                throw new PermanentDeliveryException("never");
            }

            return TransientFailures.TryGetValue(message.MessageId, out var failures) && message.DeliveryAttempt <= failures
                ? throw new InvalidOperationException("not yet")
                : Task.CompletedTask;
        }

        public Task ProcessBatchAsync(IReadOnlyList<IncomingMessage> messages, CancellationToken cancellationToken)
        {
            Record($"batch[{string.Join(",", messages.Select(m => m.MessageId))}]");
            var failing = FailBatches || messages.Any(m => Permanent.Contains(m.MessageId) || TransientFailures.ContainsKey(m.MessageId));
            return failing ? throw new InvalidOperationException("batch failed") : Task.CompletedTask;
        }

        private void Record(string call)
        {
            lock (_calls)
            {
                _calls.Add(call);
            }
        }
    }
}
