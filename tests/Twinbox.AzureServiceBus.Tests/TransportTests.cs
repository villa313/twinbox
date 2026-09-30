using Azure.Messaging.ServiceBus;
using Twinbox.Transport;

namespace Twinbox.AzureServiceBus.Tests;

public sealed class TransportTests
{
    [Theory]
    [InlineData(ServiceBusFailureReason.MessagingEntityNotFound, true)]
    [InlineData(ServiceBusFailureReason.MessageSizeExceeded, true)]
    [InlineData(ServiceBusFailureReason.ServiceBusy, false)]
    [InlineData(ServiceBusFailureReason.ServiceTimeout, false)]
    [InlineData(ServiceBusFailureReason.ServiceCommunicationProblem, false)]
    [InlineData(ServiceBusFailureReason.QuotaExceeded, false)]
    [InlineData(ServiceBusFailureReason.GeneralError, false)]
    public void ServiceBusFailures_AreClassifiedByReason(ServiceBusFailureReason reason, bool permanent)
    {
        Assert.Equal(permanent, AzureServiceBusMapping.IsPermanentSendFailure(new ServiceBusException("boom", reason)));
    }

    [Fact]
    public void UnauthorizedAccess_IsTransient()
    {
        Assert.False(AzureServiceBusMapping.IsPermanentSendFailure(new UnauthorizedAccessException("no send claim")));
    }

    [Fact]
    public void OtherExceptions_AreTransient()
    {
        Assert.False(AzureServiceBusMapping.IsPermanentSendFailure(new TimeoutException()));
        Assert.False(AzureServiceBusMapping.IsPermanentSendFailure(new InvalidOperationException()));
    }

    [Fact]
    public async Task Name_IsAzureServiceBus()
    {
        await using var transport = new AzureServiceBusTransport(new FakeServiceBusClient(), new AzureServiceBusOptions());

        Assert.Equal("azureservicebus", transport.Name);
    }

    [Fact]
    public async Task SendAsync_ReusesOneSenderPerDestination()
    {
        var client = new FakeServiceBusClient();
        await using var transport = new AzureServiceBusTransport(client, new AzureServiceBusOptions());

        await transport.SendAsync(MappingTests.Outgoing(null), default);
        await transport.SendAsync(MappingTests.Outgoing(null) with { MessageId = "msg-2" }, default);
        await transport.SendAsync(MappingTests.Outgoing(null) with { Destination = "billing" }, default);

        Assert.Equal(2, client.SendersCreated);
        Assert.Equal(["msg-1", "msg-2"], client.Senders["orders"].Sent.Select(m => m.MessageId));
        Assert.Single(client.Senders["billing"].Sent);
    }

    [Fact]
    public async Task SendAsync_WithSessionIds_SetsSessionId()
    {
        var client = new FakeServiceBusClient();
        await using var transport = new AzureServiceBusTransport(client, new AzureServiceBusOptions { SendSessionIds = true });

        await transport.SendAsync(MappingTests.Outgoing("order-1"), default);

        Assert.Equal("order-1", Assert.Single(client.Senders["orders"].Sent).SessionId);
    }

    [Fact]
    public async Task SendAsync_WrapsPermanentFailures()
    {
        var notFound = new ServiceBusException("gone", ServiceBusFailureReason.MessagingEntityNotFound);
        var client = new FakeServiceBusClient { Failure = _ => notFound };
        await using var transport = new AzureServiceBusTransport(client, new AzureServiceBusOptions());

        var error = await Assert.ThrowsAsync<PermanentDeliveryException>(() => transport.SendAsync(MappingTests.Outgoing(null), default));

        Assert.Same(notFound, error.InnerException);
        Assert.Contains("orders", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SendAsync_RethrowsTransientFailuresUnchanged()
    {
        var busy = new ServiceBusException("busy", ServiceBusFailureReason.ServiceBusy);
        var client = new FakeServiceBusClient { Failure = _ => busy };
        await using var transport = new AzureServiceBusTransport(client, new AzureServiceBusOptions());

        var error = await Assert.ThrowsAsync<ServiceBusException>(() => transport.SendAsync(MappingTests.Outgoing(null), default));

        Assert.Same(busy, error);
    }

    [Fact]
    public async Task DisposeAsync_DisposesSendersAndClient()
    {
        var client = new FakeServiceBusClient();
        var transport = new AzureServiceBusTransport(client, new AzureServiceBusOptions());
        await transport.SendAsync(MappingTests.Outgoing(null), default);

        await transport.DisposeAsync();

        Assert.True(client.Senders["orders"].Disposed);
        Assert.True(client.Disposed);
    }
}
