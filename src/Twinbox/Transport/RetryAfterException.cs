namespace Twinbox.Transport;

/// <summary>A transient failure that knows when to try again, e.g. HTTP 429 with Retry-After; the dispatcher waits at least that long.</summary>
public sealed class RetryAfterException : Exception
{
    public RetryAfterException()
    {
    }

    public RetryAfterException(string message)
        : base(message)
    {
    }

    public RetryAfterException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public RetryAfterException(string message, TimeSpan retryAfter)
        : base(message)
    {
        RetryAfter = retryAfter;
    }

    public RetryAfterException(string message, TimeSpan retryAfter, Exception innerException)
        : base(message, innerException)
    {
        RetryAfter = retryAfter;
    }

    public TimeSpan RetryAfter { get; }
}
