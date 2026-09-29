using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json.Nodes;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Twinbox.Transport;

namespace Twinbox.AmazonSqs;

/// <summary>Owns the SQS and SNS clients and caches queue URLs and topic ARNs, creating them when asked to.</summary>
internal sealed partial class AmazonSqsClients : IDisposable
{
    private readonly ILogger _logger;
    private readonly Lazy<IAmazonSQS> _sqs;
    private readonly Lazy<IAmazonSimpleNotificationService> _sns;
    private readonly ConcurrentDictionary<string, string> _queueUrls = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _topicArns = new(StringComparer.Ordinal);

    public AmazonSqsClients(IOptions<AmazonSqsOptions> options, ILogger<AmazonSqsClients> logger)
    {
        _logger = logger;
        Options = options.Value;
        _sqs = new Lazy<IAmazonSQS>(() => Options.Credentials is { } credentials
            ? new AmazonSQSClient(credentials, CreateSqsConfig(Options))
            : new AmazonSQSClient(CreateSqsConfig(Options)));
        _sns = new Lazy<IAmazonSimpleNotificationService>(() => Options.Credentials is { } credentials
            ? new AmazonSimpleNotificationServiceClient(credentials, CreateSnsConfig(Options))
            : new AmazonSimpleNotificationServiceClient(CreateSnsConfig(Options)));
    }

    public AmazonSqsOptions Options { get; }

    public IAmazonSQS Sqs => _sqs.Value;

    public IAmazonSimpleNotificationService Sns => _sns.Value;

    public async Task<string> GetQueueUrlAsync(string queue, CancellationToken cancellationToken)
    {
        if (IsUrl(queue))
        {
            return queue;
        }

        if (_queueUrls.TryGetValue(queue, out var cached))
        {
            return cached;
        }

        string url;
        try
        {
            url = (await Sqs.GetQueueUrlAsync(new GetQueueUrlRequest { QueueName = queue }, cancellationToken).ConfigureAwait(false)).QueueUrl;
        }
        catch (Exception ex) when (Options.AutoCreate && AmazonSqsErrors.IsMissingQueue(ex))
        {
            url = await CreateQueueAsync(queue, cancellationToken).ConfigureAwait(false);
        }

        _queueUrls[queue] = url;
        return url;
    }

    public void ForgetQueue(string queue) => _queueUrls.TryRemove(queue, out _);

    public async Task<string> GetTopicArnAsync(string topic, CancellationToken cancellationToken)
    {
        if (topic.StartsWith("arn:", StringComparison.Ordinal))
        {
            return topic;
        }

        if (_topicArns.TryGetValue(topic, out var cached))
        {
            return cached;
        }

        var arn = Options.AutoCreate
            ? await CreateTopicAsync(topic, cancellationToken).ConfigureAwait(false)
            : await FindTopicAsync(topic, cancellationToken).ConfigureAwait(false)
                ?? throw new NotFoundException($"SNS topic '{topic}' does not exist.");
        _topicArns[topic] = arn;
        return arn;
    }

    public async Task SendToQueueAsync(TransportMessage message, CancellationToken cancellationToken)
    {
        var url = await GetQueueUrlAsync(message.Destination, cancellationToken).ConfigureAwait(false);
        var outgoing = AmazonSqsMapping.ToOutgoing(message, AmazonSqsMapping.IsFifo(message.Destination));
        await Sqs.SendMessageAsync(AmazonSqsMapping.ToSendRequest(outgoing, url), cancellationToken).ConfigureAwait(false);
    }

    public async Task PublishAsync(TransportMessage message, string topic, CancellationToken cancellationToken)
    {
        var arn = await GetTopicArnAsync(topic, cancellationToken).ConfigureAwait(false);
        var outgoing = AmazonSqsMapping.ToOutgoing(message, AmazonSqsMapping.IsFifo(topic));
        await Sns.PublishAsync(AmazonSqsMapping.ToPublishRequest(outgoing, arn), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Lets the topic deliver into the queue and subscribes it with raw delivery, so bodies arrive unwrapped.</summary>
    public async Task SubscribeAsync(string queueUrl, string topic, CancellationToken cancellationToken)
    {
        var topicArn = await GetTopicArnAsync(topic, cancellationToken).ConfigureAwait(false);
        var attributes = await Sqs.GetQueueAttributesAsync(
            new GetQueueAttributesRequest { QueueUrl = queueUrl, AttributeNames = ["QueueArn", "Policy"] },
            cancellationToken).ConfigureAwait(false);
        var queueArn = attributes.Attributes["QueueArn"];
        attributes.Attributes.TryGetValue("Policy", out var policy);

        if (AllowTopic(policy, queueArn, topicArn) is { } updated)
        {
            await Sqs.SetQueueAttributesAsync(
                new SetQueueAttributesRequest { QueueUrl = queueUrl, Attributes = new() { ["Policy"] = updated } },
                cancellationToken).ConfigureAwait(false);
        }

        await Sns.SubscribeAsync(
            new SubscribeRequest
            {
                TopicArn = topicArn,
                Protocol = "sqs",
                Endpoint = queueArn,
                Attributes = new() { ["RawMessageDelivery"] = "true" },
                ReturnSubscriptionArn = true,
            },
            cancellationToken).ConfigureAwait(false);
        LogSubscribed(queueArn, topicArn);
    }

    public void Dispose()
    {
        if (_sqs.IsValueCreated)
        {
            _sqs.Value.Dispose();
        }

        if (_sns.IsValueCreated)
        {
            _sns.Value.Dispose();
        }
    }

    internal static AmazonSQSConfig CreateSqsConfig(AmazonSqsOptions options)
    {
        var config = new AmazonSQSConfig();
        ApplyEndpoint(config, options);
        options.ConfigureSqs?.Invoke(config);
        return config;
    }

    internal static AmazonSimpleNotificationServiceConfig CreateSnsConfig(AmazonSqsOptions options)
    {
        var config = new AmazonSimpleNotificationServiceConfig();
        ApplyEndpoint(config, options);
        options.ConfigureSns?.Invoke(config);
        return config;
    }

    /// <summary>Adds a statement allowing the topic to send to the queue; null when the policy already mentions the topic.</summary>
    internal static string? AllowTopic(string? policy, string queueArn, string topicArn)
    {
        if (policy is not null && policy.Contains(topicArn, StringComparison.Ordinal))
        {
            return null;
        }

        var document = (string.IsNullOrWhiteSpace(policy) ? null : JsonNode.Parse(policy) as JsonObject)
            ?? new JsonObject { ["Version"] = "2012-10-17" };
        if (document["Statement"] is not JsonArray statements)
        {
            statements = document["Statement"] is JsonObject single ? [single.DeepClone()] : [];
            document["Statement"] = statements;
        }

        statements.Add((JsonNode)new JsonObject
        {
            ["Effect"] = "Allow",
            ["Principal"] = new JsonObject { ["Service"] = "sns.amazonaws.com" },
            ["Action"] = "sqs:SendMessage",
            ["Resource"] = queueArn,
            ["Condition"] = new JsonObject { ["ArnEquals"] = new JsonObject { ["aws:SourceArn"] = topicArn } },
        });
        return document.ToJsonString();
    }

    private static void ApplyEndpoint(ClientConfig config, AmazonSqsOptions options)
    {
        if (options.ServiceUrl is { } serviceUrl)
        {
            config.ServiceURL = serviceUrl.AbsoluteUri;
            if (options.Region is { } region)
            {
                config.AuthenticationRegion = region;
            }
        }
        else if (options.Region is { } region)
        {
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(region);
        }
    }

    private static bool IsUrl(string queue) =>
        queue.StartsWith("https://", StringComparison.OrdinalIgnoreCase) || queue.StartsWith("http://", StringComparison.OrdinalIgnoreCase);

    private async Task<string> CreateQueueAsync(string queue, CancellationToken cancellationToken)
    {
        var attributes = new Dictionary<string, string>
        {
            ["VisibilityTimeout"] = ((int)Options.VisibilityTimeout.TotalSeconds).ToString(CultureInfo.InvariantCulture),
        };
        if (AmazonSqsMapping.IsFifo(queue))
        {
            attributes["FifoQueue"] = "true";
        }

        var response = await Sqs.CreateQueueAsync(new CreateQueueRequest { QueueName = queue, Attributes = attributes }, cancellationToken)
            .ConfigureAwait(false);
        LogQueueCreated(queue);
        return response.QueueUrl;
    }

    private async Task<string> CreateTopicAsync(string topic, CancellationToken cancellationToken)
    {
        var request = new CreateTopicRequest { Name = topic };
        if (AmazonSqsMapping.IsFifo(topic))
        {
            request.Attributes = new() { ["FifoTopic"] = "true" };
        }

        // CreateTopic is idempotent, so it doubles as the lookup.
        var response = await Sns.CreateTopicAsync(request, cancellationToken).ConfigureAwait(false);
        LogTopicEnsured(topic, response.TopicArn);
        return response.TopicArn;
    }

    private async Task<string?> FindTopicAsync(string topic, CancellationToken cancellationToken)
    {
        var suffix = ":" + topic;
        string? nextToken = null;
        do
        {
            var page = await Sns.ListTopicsAsync(new ListTopicsRequest { NextToken = nextToken }, cancellationToken).ConfigureAwait(false);
            foreach (var candidate in page.Topics ?? [])
            {
                if (candidate.TopicArn.EndsWith(suffix, StringComparison.Ordinal))
                {
                    return candidate.TopicArn;
                }
            }

            nextToken = page.NextToken;
        }
        while (!string.IsNullOrEmpty(nextToken));

        return null;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created SQS queue {Queue}.")]
    private partial void LogQueueCreated(string queue);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Ensured SNS topic {Topic} ({TopicArn}).")]
    private partial void LogTopicEnsured(string topic, string topicArn);

    [LoggerMessage(Level = LogLevel.Information, Message = "Subscribed SQS queue {QueueArn} to SNS topic {TopicArn}.")]
    private partial void LogSubscribed(string queueArn, string topicArn);
}
