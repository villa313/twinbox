using DotPulsar.Exceptions;

namespace Twinbox.Pulsar.Tests;

public sealed class ErrorClassificationTests
{
    [Theory]
    [InlineData(nameof(TopicNotFoundException))]
    [InlineData(nameof(InvalidTopicNameException))]
    [InlineData(nameof(TooLargeMessageException))]
    [InlineData(nameof(TopicTerminatedException))]
    [InlineData(nameof(IncompatibleSchemaException))]
    [InlineData(nameof(NotAllowedException))]
    public void RejectionsRetryingCannotFix_ArePermanent(string error)
    {
        Assert.True(PulsarErrors.IsPermanent(Create(error)));
        Assert.True(PulsarErrors.IsPermanent(new ProducerFaultedException(Create(error))));
    }

    [Theory]
    [InlineData(nameof(AuthenticationException))]
    [InlineData(nameof(AuthorizationException))]
    [InlineData(nameof(ServiceNotReadyException))]
    [InlineData(nameof(TooManyRequestsException))]
    [InlineData(nameof(PersistenceException))]
    [InlineData(nameof(ProducerBlockedQuotaExceededException))]
    [InlineData(nameof(MetadataException))]
    [InlineData(nameof(ProducerFencedException))]
    [InlineData(nameof(TimeoutException))]
    [InlineData(nameof(OperationCanceledException))]
    [InlineData(nameof(IOException))]
    public void BrokerHiccupsAndAccessErrors_AreTransient(string error)
    {
        Assert.False(PulsarErrors.IsPermanent(Create(error)));
        Assert.False(PulsarErrors.IsPermanent(new ProducerFaultedException(Create(error))));
    }

    [Fact]
    public void FaultWithoutCause_IsTransient() => Assert.False(PulsarErrors.IsPermanent(new ProducerFaultedException()));

    [Fact]
    public void Cause_UnwrapsFaultsOnly()
    {
        var cause = new TopicNotFoundException("missing");

        Assert.Same(cause, PulsarErrors.Cause(new ProducerFaultedException(cause)));
        Assert.Same(cause, PulsarErrors.Cause(new ConsumerFaultedException(cause)));
        Assert.Same(cause, PulsarErrors.Cause(cause));
    }

    private static Exception Create(string error) => error switch
    {
        nameof(TopicNotFoundException) => new TopicNotFoundException("Topic does not exist"),
        nameof(InvalidTopicNameException) => new InvalidTopicNameException("Invalid topic name"),
        nameof(AuthorizationException) => new AuthorizationException("Client is not authorized to Produce"),
        nameof(TooLargeMessageException) => new TooLargeMessageException(6_000_000, 5_243_904),
        nameof(TopicTerminatedException) => new TopicTerminatedException("Topic was already terminated"),
        nameof(IncompatibleSchemaException) => new IncompatibleSchemaException("Trying to create a producer with an incompatible schema"),
        nameof(NotAllowedException) => new NotAllowedException("Producer is not allowed"),
        nameof(AuthenticationException) => new AuthenticationException("Token expired"),
        nameof(ServiceNotReadyException) => new ServiceNotReadyException("Namespace bundle is being unloaded"),
        nameof(TooManyRequestsException) => new TooManyRequestsException("Too many lookups"),
        nameof(PersistenceException) => new PersistenceException("Error writing to BookKeeper"),
        nameof(ProducerBlockedQuotaExceededException) => new ProducerBlockedQuotaExceededException("Backlog quota exceeded"),
        nameof(MetadataException) => new MetadataException("Metadata store unavailable"),
        nameof(ProducerFencedException) => new ProducerFencedException("Fenced by an exclusive producer"),
        nameof(TimeoutException) => new TimeoutException(),
        nameof(OperationCanceledException) => new OperationCanceledException(),
        nameof(IOException) => new IOException("Connection reset"),
        _ => throw new ArgumentOutOfRangeException(nameof(error), error, null),
    };
}
