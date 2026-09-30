using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Twinbox.RabbitMQ;

namespace Twinbox.RabbitMQ.Tests;

public sealed class ErrorClassificationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReturnedMessage_IsPermanentOnlyWhenConfigured(bool deadLetterUnroutable)
    {
        var returned = new PublishReturnException(1, "unroutable", "sales", "order-placed", Constants.NoRoute, "NO_ROUTE");

        Assert.Equal(deadLetterUnroutable, RabbitMQErrors.IsPermanent(returned, deadLetterUnroutable));
        Assert.True(RabbitMQErrors.IsUnroutable(returned));
        Assert.Contains("unroutable", RabbitMQErrors.Describe(returned, "sales", "order-placed"), StringComparison.Ordinal);
    }

    [Fact]
    public void BrokerNack_IsTransient() =>
        Assert.False(RabbitMQErrors.IsPermanent(new PublishException(1, isReturn: false), unroutableIsPermanent: true));

    [Theory]
    [InlineData(Constants.NotFound, true)]
    [InlineData(Constants.AccessRefused, false)]
    [InlineData(Constants.ConnectionForced, false)]
    [InlineData(Constants.InternalError, false)]
    public void ChannelClose_IsPermanentOnlyForMissingResources(ushort replyCode, bool permanent)
    {
        var closed = new OperationInterruptedException(new ShutdownEventArgs(ShutdownInitiator.Peer, replyCode, "closed"));

        Assert.Equal(permanent, RabbitMQErrors.IsPermanent(closed, unroutableIsPermanent: false));
    }

    [Fact]
    public void LostConnection_IsTransient() =>
        Assert.False(RabbitMQErrors.IsPermanent(
            new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Library, Constants.ConnectionForced, "lost")),
            unroutableIsPermanent: true));

    [Fact]
    public void UnreachableBrokerAndTimeouts_AreTransient()
    {
        Assert.False(RabbitMQErrors.IsPermanent(new BrokerUnreachableException(new IOException("refused")), unroutableIsPermanent: true));
        Assert.False(RabbitMQErrors.IsPermanent(new TimeoutException(), unroutableIsPermanent: true));
    }
}
