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

        Assert.Equal(deadLetterUnroutable, RabbitMqErrors.IsPermanent(returned, deadLetterUnroutable));
        Assert.True(RabbitMqErrors.IsUnroutable(returned));
        Assert.Contains("unroutable", RabbitMqErrors.Describe(returned, "sales", "order-placed"), StringComparison.Ordinal);
    }

    [Fact]
    public void BrokerNack_IsTransient() =>
        Assert.False(RabbitMqErrors.IsPermanent(new PublishException(1, isReturn: false), unroutableIsPermanent: true));

    [Theory]
    [InlineData(Constants.NotFound, true)]
    [InlineData(Constants.AccessRefused, true)]
    [InlineData(Constants.ConnectionForced, false)]
    [InlineData(Constants.InternalError, false)]
    public void ChannelClose_IsPermanentOnlyForMissingOrForbiddenResources(ushort replyCode, bool permanent)
    {
        var closed = new OperationInterruptedException(new ShutdownEventArgs(ShutdownInitiator.Peer, replyCode, "closed"));

        Assert.Equal(permanent, RabbitMqErrors.IsPermanent(closed, unroutableIsPermanent: false));
    }

    [Fact]
    public void LostConnection_IsTransient() =>
        Assert.False(RabbitMqErrors.IsPermanent(
            new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Library, Constants.ConnectionForced, "lost")),
            unroutableIsPermanent: true));

    [Fact]
    public void UnreachableBrokerAndTimeouts_AreTransient()
    {
        Assert.False(RabbitMqErrors.IsPermanent(new BrokerUnreachableException(new IOException("refused")), unroutableIsPermanent: true));
        Assert.False(RabbitMqErrors.IsPermanent(new TimeoutException(), unroutableIsPermanent: true));
    }
}
