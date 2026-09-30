using System.Text.Json.Nodes;
using Amazon;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.AmazonSqs.Tests;

public sealed class RegistrationTests
{
    private const string QueueArn = "arn:aws:sqs:us-east-1:000000000000:billing";
    private const string TopicArn = "arn:aws:sns:us-east-1:000000000000:orders";

    [Fact]
    public async Task UseAmazonSqs_RegistersTransportAndReceiver()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseAmazonSqs(o => o.Listen("orders").Listen("billing", "sns:payments")))
            .BuildServiceProvider();

        var transport = Assert.Single(services.GetServices<ITransport>());
        Assert.Equal(AmazonSqsTransport.TransportName, transport.Name);
        Assert.Same(services.GetRequiredService<AmazonSqsTransport>(), transport);
        Assert.Contains(services.GetServices<IHostedService>(), s => s is AmazonSqsReceiverService);
        Assert.Equal(
            [new AmazonSqsListener("orders", null), new AmazonSqsListener("billing", "payments")],
            services.GetRequiredService<IOptions<AmazonSqsOptions>>().Value.Listeners);
    }

    [Fact]
    public async Task RegionOverload_CanStillRegisterListeners()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseAmazonSqs("eu-west-1", o => o.Listen("orders")))
            .BuildServiceProvider();

        var options = services.GetRequiredService<IOptions<AmazonSqsOptions>>().Value;
        Assert.Equal("eu-west-1", options.Region);
        Assert.Equal([new AmazonSqsListener("orders", null)], options.Listeners);
    }

    [Fact]
    public async Task RegionOverload_ConfiguresIt()
    {
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddTwinbox(b => b.UseInMemoryStore().UseAmazonSqs("eu-west-1"))
            .BuildServiceProvider();

        Assert.Equal("eu-west-1", services.GetRequiredService<IOptions<AmazonSqsOptions>>().Value.Region);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void RegionOverload_RejectsBlankRegions(string region) =>
        Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddTwinbox(b => b.UseAmazonSqs(region)));

    private static readonly Dictionary<string, Action<AmazonSqsOptions>> InvalidSettings = new()
    {
        ["wait-too-long"] = o => o.WaitTimeSeconds = 21,
        ["no-messages"] = o => o.MaxNumberOfMessages = 0,
        ["too-many-messages"] = o => o.MaxNumberOfMessages = 11,
        ["no-concurrency"] = o => o.MaxConcurrency = 0,
        ["no-visibility"] = o => o.VisibilityTimeout = TimeSpan.Zero,
        ["visibility-too-long"] = o => o.VisibilityTimeout = TimeSpan.FromHours(13),
        ["retry-above-cap"] = o => o.RetryDelay = TimeSpan.FromMinutes(10),
        ["cap-too-long"] = o => o.MaxRetryDelay = TimeSpan.FromHours(13),
        ["blank-dead-letter-queue"] = o => o.DeadLetterQueue = " ",
        ["blank-region"] = o => o.Region = " ",
    };

    public static TheoryData<string> InvalidOptions => [.. InvalidSettings.Keys];

    [Theory]
    [MemberData(nameof(InvalidOptions))]
    public async Task InvalidOptions_FailValidation(string setting)
    {
        await using var services = new ServiceCollection()
            .AddTwinbox(b => b.UseInMemoryStore().UseAmazonSqs(InvalidSettings[setting]))
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<AmazonSqsOptions>>().Value);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void ListenerNames_RejectBlanks(string name)
    {
        var options = new AmazonSqsOptions();

        Assert.ThrowsAny<ArgumentException>(() => options.Listen(name));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen(name, "topic"));
        Assert.ThrowsAny<ArgumentException>(() => options.Listen("queue", name));
    }

    [Fact]
    public void ClientConfig_PointsAtServiceUrlWithTheRegionForSigning()
    {
        var options = new AmazonSqsOptions
        {
            ServiceUrl = new Uri("http://localhost:4566"),
            Region = "eu-central-1",
            ConfigureSqs = c => c.MaxErrorRetry = 7,
            ConfigureSns = c => c.MaxErrorRetry = 5,
        };

        var sqs = AmazonSqsClients.CreateSqsConfig(options);
        var sns = AmazonSqsClients.CreateSnsConfig(options);

        Assert.Equal("http://localhost:4566/", sqs.ServiceURL);
        Assert.Equal("eu-central-1", sqs.AuthenticationRegion);
        Assert.Equal(7, sqs.MaxErrorRetry);
        Assert.Equal("http://localhost:4566/", sns.ServiceURL);
        Assert.Equal(5, sns.MaxErrorRetry);
    }

    [Fact]
    public void ClientConfig_UsesTheRegionEndpointWithoutServiceUrl()
    {
        var config = AmazonSqsClients.CreateSqsConfig(new AmazonSqsOptions { Region = "ap-southeast-2" });

        Assert.Equal(RegionEndpoint.APSoutheast2, config.RegionEndpoint);
    }

    [Theory]
    [InlineData(1, 100)]
    [InlineData(2, 200)]
    [InlineData(4, 800)]
    [InlineData(5, 1000)]
    [InlineData(500, 1000)]
    public void RetryDelay_DoublesUpToItsCap(int attempt, int expectedMilliseconds)
    {
        var options = new AmazonSqsOptions { RetryDelay = TimeSpan.FromMilliseconds(100), MaxRetryDelay = TimeSpan.FromSeconds(1) };

        Assert.Equal(TimeSpan.FromMilliseconds(expectedMilliseconds), AmazonSqsReceiverService.RetryDelay(options, attempt));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(100, 1)]
    [InlineData(1000, 1)]
    [InlineData(1500, 2)]
    [InlineData(100_000_000, 43_200)]
    public void VisibilitySeconds_RoundUpAndStayWithinTheSqsLimit(int milliseconds, int expected) =>
        Assert.Equal(expected, AmazonSqsReceiverService.ToVisibilitySeconds(TimeSpan.FromMilliseconds(milliseconds)));

    [Fact]
    public void Partition_KeepsFifoGroupsTogetherInReceiveOrder()
    {
        Message[] messages = [Grouped("1", "a"), Grouped("2", "b"), Grouped("3", "a"), Grouped("4", "b"), Grouped("5", "c")];

        var fifo = AmazonSqsReceiverService.Partition(messages, fifo: true);
        var standard = AmazonSqsReceiverService.Partition(messages, fifo: false);

        Assert.Equal([["1", "3"], ["2", "4"], ["5"]], fifo.Select(u => u.Select(m => m.MessageId).ToArray()).ToArray());
        Assert.Equal(5, standard.Count);
        Assert.All(standard, unit => Assert.Single(unit));
    }

    [Fact]
    public void AllowTopic_CreatesAPolicyForAQueueWithoutOne()
    {
        var policy = JsonNode.Parse(AmazonSqsClients.AllowTopic(null, QueueArn, TopicArn)!)!;

        var statement = Assert.Single(policy["Statement"]!.AsArray())!;
        Assert.Equal("sqs:SendMessage", statement["Action"]!.GetValue<string>());
        Assert.Equal(QueueArn, statement["Resource"]!.GetValue<string>());
        Assert.Equal(TopicArn, statement["Condition"]!["ArnEquals"]!["aws:SourceArn"]!.GetValue<string>());
    }

    [Fact]
    public void AllowTopic_AppendsToAnExistingPolicy()
    {
        const string existing = """{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":"*","Action":"sqs:ReceiveMessage"}}""";

        var policy = JsonNode.Parse(AmazonSqsClients.AllowTopic(existing, QueueArn, TopicArn)!)!;

        var statements = policy["Statement"]!.AsArray();
        Assert.Equal(2, statements.Count);
        Assert.Equal("sqs:ReceiveMessage", statements[0]!["Action"]!.GetValue<string>());
    }

    [Fact]
    public void AllowTopic_LeavesAPolicyThatAlreadyAllowsTheTopic()
    {
        var existing = AmazonSqsClients.AllowTopic(null, QueueArn, TopicArn);

        Assert.Null(AmazonSqsClients.AllowTopic(existing, QueueArn, TopicArn));
    }

    private static Message Grouped(string id, string group) => new() { MessageId = id, Attributes = new() { ["MessageGroupId"] = group } };
}
