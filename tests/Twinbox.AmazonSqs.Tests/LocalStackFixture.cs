using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Testcontainers.LocalStack;

namespace Twinbox.AmazonSqs.Tests;

public sealed class LocalStackFixture : IAsyncLifetime
{
    public const string Region = "us-east-1";

    private readonly LocalStackContainer _container = new LocalStackBuilder("localstack/localstack:3")
        .WithEnvironment("SERVICES", "sqs,sns")
        .Build();

    private AmazonSQSClient? _sqs;

    public Uri ServiceUrl => new(_container.GetConnectionString());

    public static AWSCredentials Credentials { get; } = new BasicAWSCredentials("test", "test");

    public IAmazonSQS Sqs => _sqs ??= new AmazonSQSClient(Credentials, new AmazonSQSConfig { ServiceURL = ServiceUrl.AbsoluteUri, AuthenticationRegion = Region });

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync()
    {
        _sqs?.Dispose();
        await _container.DisposeAsync();
    }

    public async Task<string> CreateQueueAsync(string name)
    {
        var attributes = new Dictionary<string, string>();
        if (name.EndsWith(".fifo", StringComparison.Ordinal))
        {
            attributes["FifoQueue"] = "true";
        }

        return (await Sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = name, Attributes = attributes })).QueueUrl;
    }

    public async Task<Message> ReceiveOneAsync(string queue, TimeSpan timeout)
    {
        var url = (await Sqs.GetQueueUrlAsync(queue)).QueueUrl;
        using var cts = new CancellationTokenSource(timeout);
        while (true)
        {
            var response = await Sqs.ReceiveMessageAsync(
                new ReceiveMessageRequest { QueueUrl = url, WaitTimeSeconds = 1, MessageAttributeNames = ["All"], MessageSystemAttributeNames = ["All"] },
                cts.Token);
            if (response.Messages is [var message, ..])
            {
                return message;
            }
        }
    }

    /// <summary>Visible plus in-flight messages; zero once everything received has been deleted.</summary>
    public async Task<int> CountAsync(string queue)
    {
        var url = (await Sqs.GetQueueUrlAsync(queue)).QueueUrl;
        var attributes = await Sqs.GetQueueAttributesAsync(
            new GetQueueAttributesRequest { QueueUrl = url, AttributeNames = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] });
        return attributes.ApproximateNumberOfMessages + attributes.ApproximateNumberOfMessagesNotVisible;
    }
}
