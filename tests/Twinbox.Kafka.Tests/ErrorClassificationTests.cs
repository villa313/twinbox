using Confluent.Kafka;

namespace Twinbox.Kafka.Tests;

public sealed class ErrorClassificationTests
{
    [Theory]
    [InlineData(ErrorCode.UnknownTopicOrPart)]
    [InlineData(ErrorCode.Local_UnknownTopic)]
    [InlineData(ErrorCode.TopicException)]
    [InlineData(ErrorCode.MsgSizeTooLarge)]
    [InlineData(ErrorCode.InvalidConfig)]
    [InlineData(ErrorCode.Local_InvalidArg)]
    public void RejectionsRetryingCannotFix_ArePermanent(ErrorCode code)
    {
        Assert.True(KafkaErrors.IsPermanent(new KafkaException(code)));
        Assert.True(KafkaErrors.IsPermanent(ProduceError(code)));
    }

    [Theory]
    [InlineData(ErrorCode.Local_Transport)]
    [InlineData(ErrorCode.Local_AllBrokersDown)]
    [InlineData(ErrorCode.Local_MsgTimedOut)]
    [InlineData(ErrorCode.Local_QueueFull)]
    [InlineData(ErrorCode.LeaderNotAvailable)]
    [InlineData(ErrorCode.NotLeaderForPartition)]
    [InlineData(ErrorCode.NotEnoughReplicas)]
    [InlineData(ErrorCode.RequestTimedOut)]
    [InlineData(ErrorCode.TopicAuthorizationFailed)]
    [InlineData(ErrorCode.ClusterAuthorizationFailed)]
    public void BrokerHiccupsAndAccessErrors_AreTransient(ErrorCode code)
    {
        Assert.False(KafkaErrors.IsPermanent(new KafkaException(code)));
        Assert.False(KafkaErrors.IsPermanent(ProduceError(code)));
    }

    [Fact]
    public void NonKafkaFailures_AreTransient()
    {
        Assert.False(KafkaErrors.IsPermanent(new TimeoutException()));
        Assert.False(KafkaErrors.IsPermanent(new OperationCanceledException()));
    }

    [Fact]
    public void FatalErrors_AreRecognised()
    {
        Assert.True(KafkaErrors.IsFatal(new KafkaException(new Error(ErrorCode.Local_Fatal, "fenced", isFatal: true))));
        Assert.False(KafkaErrors.IsFatal(new KafkaException(ErrorCode.Local_Transport)));
        Assert.False(KafkaErrors.IsFatal(new InvalidOperationException()));
    }

    private static ProduceException<string?, byte[]> ProduceError(ErrorCode code) =>
        new(new Error(code), new DeliveryResult<string?, byte[]>());
}
