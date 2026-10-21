using DistributedScheduler.Domain;

namespace DistributedScheduler.Tests;

public class BackoffTests
{
    private static readonly TimeSpan Base = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan Cap = TimeSpan.FromMinutes(5);

    [Fact]
    public void Doubles_each_attempt() =>
        Assert.Equal(TimeSpan.FromSeconds(8), Backoff.Delay(3, Base, Cap, jitter: 1.0));

    [Fact]
    public void Is_capped() =>
        Assert.Equal(Cap, Backoff.Delay(30, Base, Cap, jitter: 1.0));

    [Fact]
    public void Jitter_shrinks_delay_to_half() =>
        Assert.Equal(TimeSpan.FromSeconds(1), Backoff.Delay(1, Base, Cap, jitter: 0.0));
}