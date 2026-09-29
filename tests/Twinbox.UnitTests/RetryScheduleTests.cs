using Twinbox.Dispatch;

namespace Twinbox.UnitTests;

public sealed class RetryScheduleTests
{
    private static readonly RetryOptions Options = new()
    {
        InitialDelay = TimeSpan.FromSeconds(1),
        MaxDelay = TimeSpan.FromSeconds(60),
    };

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(4, 8)]
    [InlineData(20, 60)]
    public void GetDelay_StaysBetweenHalfAndFullCeiling(int attempt, int ceilingSeconds)
    {
        var random = new Random(7);
        for (var i = 0; i < 100; i++)
        {
            var delay = RetrySchedule.GetDelay(Options, attempt, random);

            Assert.InRange(delay, TimeSpan.FromSeconds(ceilingSeconds / 2.0), TimeSpan.FromSeconds(ceilingSeconds));
        }
    }

    [Fact]
    public void GetDelay_HugeAttemptCount_DoesNotOverflow()
    {
        var delay = RetrySchedule.GetDelay(Options, int.MaxValue, new Random(1));

        Assert.InRange(delay, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(60));
    }
}
