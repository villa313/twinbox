namespace Twinbox.Dispatch;

internal sealed class CircuitBreaker(CircuitBreakerOptions options)
{
    private readonly object _gate = new();
    private int _consecutiveFailures;
    private DateTimeOffset? _openUntil;

    public bool TryAllow(DateTimeOffset now, out DateTimeOffset retryAt)
    {
        lock (_gate)
        {
            if (_openUntil is { } until && until > now)
            {
                retryAt = until;
                return false;
            }

            retryAt = now;
            return true;
        }
    }

    public void RecordSuccess()
    {
        lock (_gate)
        {
            _consecutiveFailures = 0;
            _openUntil = null;
        }
    }

    public void RecordFailure(DateTimeOffset now)
    {
        if (options.FailureThreshold <= 0)
        {
            return;
        }

        lock (_gate)
        {
            _consecutiveFailures++;
            if (_consecutiveFailures >= options.FailureThreshold)
            {
                _openUntil = now + options.BreakDuration;

                // Half-open: the first failure after the break reopens immediately.
                _consecutiveFailures = options.FailureThreshold - 1;
            }
        }
    }
}
