using StackExchange.Redis;

namespace Twinbox.RedisStreams.Tests;

public sealed class ErrorClassificationTests
{
    [Theory]
    [InlineData("WRONGTYPE Operation against a key holding the wrong kind of value")]
    [InlineData("NOPERM User app has no permissions to run the 'xadd' command")]
    [InlineData("NOPERM No permissions to access a key")]
    public void RejectionsRetryingCannotFix_ArePermanent(string message) =>
        Assert.True(RedisStreamsErrors.IsPermanent(new RedisServerException(message)));

    [Theory]
    [InlineData("OOM command not allowed when used memory > 'maxmemory'.")]
    [InlineData("READONLY You can't write against a read only replica.")]
    [InlineData("LOADING Redis is loading the dataset in memory")]
    [InlineData("BUSY Redis is busy running a script.")]
    [InlineData("CLUSTERDOWN The cluster is down")]
    public void ServerConditionsThatPass_AreTransient(string message) =>
        Assert.False(RedisStreamsErrors.IsPermanent(new RedisServerException(message)));

    [Fact]
    public void ConnectionFailuresAndTimeouts_AreTransient()
    {
        Assert.False(RedisStreamsErrors.IsPermanent(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down")));
        Assert.False(RedisStreamsErrors.IsPermanent(new RedisTimeoutException("slow", CommandStatus.Sent)));
        Assert.False(RedisStreamsErrors.IsPermanent(new TimeoutException()));
        Assert.False(RedisStreamsErrors.IsPermanent(new OperationCanceledException()));
    }

    [Fact]
    public void ExistingGroup_IsRecognised()
    {
        Assert.True(RedisStreamsErrors.IsGroupExisting(new RedisServerException("BUSYGROUP Consumer Group name already exists")));
        Assert.False(RedisStreamsErrors.IsGroupExisting(new RedisServerException("NOGROUP No such key 'orders'")));
        Assert.False(RedisStreamsErrors.IsGroupExisting(new InvalidOperationException("BUSYGROUP")));
    }
}
