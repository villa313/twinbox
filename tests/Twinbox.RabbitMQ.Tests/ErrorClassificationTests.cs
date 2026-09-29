using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Twinbox.RabbitMQ;

namespace Twinbox.RabbitMQ.Tests;

public sealed class ErrorClassificationTests
{
    [Fact]
    public void ReturnedMessage_IsPermanent()
    {
        var returned = new PublishReturnException(1, "unroutable", "sales", "order-placed", Constants.NoRoute, "NO_ROUTE");

        Assert.True(RabbitMqErrors.IsPermanent(returned));
        Assert.Contains("unroutable", RabbitMqErrors.Describe(returned, "sales", "order-placed"), StringComparison.Ordinal);
    }

    [Fact]
    public void BrokerNack_IsTransient() =>
        Assert.False(RabbitMqErrors.IsPermanent(new PublishException(1, isReturn: false)));

    [Theory]
    [InlineData(Constants.NotFound, true)]
    [InlineData(Constants.AccessRefused, true)]
    [InlineData(Constants.ConnectionForced, false)]
    [InlineData(Constants.InternalError, false)]
    public void ChannelClose_IsPermanentOnlyForMissingOrForbiddenResources(ushort replyCode, bool permanent)
    {
        var closed = new OperationInterruptedException(new ShutdownEventArgs(ShutdownInitiator.Peer, replyCode, "closed"));

        Assert.Equal(permanent, RabbitMqErrors.IsPermanent(closed));
    }

    [Fact]
    public void LostConnection_IsTransient() =>
        Assert.False(RabbitMqErrors.IsPermanent(
            new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Library, Constants.ConnectionForced, "lost"))));

    [Fact]
    public void UnreachableBrokerAndTimeouts_AreTransient()
    {
        Assert.False(RabbitMqErrors.IsPermanent(new BrokerUnreachableException(new IOException("refused"))));
        Assert.False(RabbitMqErrors.IsPermanent(new TimeoutException()));
    }
}
