using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Twinbox.AmazonSqs;
using Twinbox.Transport;

namespace Twinbox;

public static class AmazonSqsTwinboxBuilderExtensions
{
    private static readonly TimeSpan MaxVisibilityTimeout = TimeSpan.FromHours(12);

    [SuppressMessage("ApiDesign", "RS0026", Justification = "The overloads differ by a required first parameter, so calls cannot be ambiguous.")]
    public static TwinboxBuilder UseAmazonSqs(this TwinboxBuilder builder, string region, Action<AmazonSqsOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(region);
        return builder.UseAmazonSqs(options =>
        {
            options.Region = region;
            configure?.Invoke(options);
        });
    }

    /// <summary>Sends through SQS and SNS, and consumes the queues registered with <see cref="AmazonSqsOptions.Listen(string)"/>.</summary>
    public static TwinboxBuilder UseAmazonSqs(this TwinboxBuilder builder, Action<AmazonSqsOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.AddOptions<AmazonSqsOptions>()
            .Configure(configure)
            .Validate(o => o.Region is null || !string.IsNullOrWhiteSpace(o.Region), "Amazon SQS Region cannot be blank.")
            .Validate(o => o.VisibilityTimeout >= TimeSpan.FromSeconds(1) && o.VisibilityTimeout <= MaxVisibilityTimeout, "Amazon SQS VisibilityTimeout must be between 1 second and 12 hours.")
            .Validate(o => o.WaitTimeSeconds is >= 0 and <= 20, "Amazon SQS WaitTimeSeconds must be between 0 and 20.")
            .Validate(o => o.MaxNumberOfMessages is >= 1 and <= 10, "Amazon SQS MaxNumberOfMessages must be between 1 and 10.")
            .Validate(o => o.MaxConcurrency > 0, "Amazon SQS MaxConcurrency must be positive.")
            .Validate(o => o.RetryDelay > TimeSpan.Zero, "Amazon SQS RetryDelay must be positive.")
            .Validate(o => o.MaxRetryDelay >= o.RetryDelay && o.MaxRetryDelay <= MaxVisibilityTimeout, "Amazon SQS MaxRetryDelay must be between RetryDelay and 12 hours.")
            .Validate(o => o.DeadLetterQueue is null || !string.IsNullOrWhiteSpace(o.DeadLetterQueue), "Amazon SQS DeadLetterQueue cannot be blank.");

        builder.Services.TryAddSingleton<AmazonSqsClients>();
        builder.Services.TryAddSingleton(sp => new AmazonSqsTransport(sp.GetRequiredService<AmazonSqsClients>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<ITransport, AmazonSqsTransport>(sp => sp.GetRequiredService<AmazonSqsTransport>()));
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, AmazonSqsReceiverService>());
        return builder;
    }
}
