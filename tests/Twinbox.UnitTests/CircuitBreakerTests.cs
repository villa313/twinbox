using Twinbox.Dispatch;

namespace Twinbox.UnitTests;

public sealed class CircuitBreakerTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public void RecordFailure_AtThreshold_OpensForBreakDuration()
    {
        var breaker = new CircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 2, BreakDuration = TimeSpan.FromSeconds(30) });

        breaker.RecordFailure(Now);
        Assert.True(breaker.TryAllow(Now, out _));

        breaker.RecordFailure(Now);
        Assert.False(breaker.TryAllow(Now.AddSeconds(29), out var retryAt));
        Assert.Equal(Now.AddSeconds(30), retryAt);
        Assert.True(breaker.TryAllow(Now.AddSeconds(30), out _));
    }

    [Fact]
    public void RecordFailure_AfterBreakElapses_ReopensOnFirstFailure()
    {
        var breaker = new CircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 3, BreakDuration = TimeSpan.FromSeconds(10) });
        for (var i = 0; i < 3; i++)
        {
            breaker.RecordFailure(Now);
        }

        var later = Now.AddSeconds(11);
        breaker.RecordFailure(later);

        Assert.False(breaker.TryAllow(later, out _));
    }

    [Fact]
    public void RecordSuccess_ResetsFailureCount()
    {
        var breaker = new CircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 2 });

        breaker.RecordFailure(Now);
        breaker.RecordSuccess();
        breaker.RecordFailure(Now);

        Assert.True(breaker.TryAllow(Now, out _));
    }

    [Fact]
    public void RecordFailure_ZeroThreshold_NeverOpens()
    {
        var breaker = new CircuitBreaker(new CircuitBreakerOptions { FailureThreshold = 0 });
        for (var i = 0; i < 100; i++)
        {
            breaker.RecordFailure(Now);
        }

        Assert.True(breaker.TryAllow(Now, out _));
    }
}
