using DistributedScheduler.Domain;
using DistributedScheduler.Worker;

namespace DistributedScheduler.Tests;

public class QueueTests : PgTestBase
{
    [Fact]
    public async Task Queue_concurrency_cap_is_respected()
    {
        for (var i = 0; i < 12; i++)
            await Repo.EnqueueJsonAsync("ProbeJob", null, new JobOptions { Queue = "capped" });

        using var host = BuildHost(o =>
        {
            o.Queues["capped"] = new QueueOptions { Concurrency = 2, BatchSize = 2 };
            o.PollInterval = TimeSpan.FromMilliseconds(100);
        });
        await host.StartAsync();

        await WaitForAsync(
            async () => await CountAsync("SELECT count(*) FROM jobs WHERE queue = 'capped' AND status = 2") == 12,
            TimeSpan.FromSeconds(30), "all capped jobs to finish");
        await host.StopAsync();

        Assert.Equal(2, ConcurrencyProbe.MaxSeen("capped"));   // never more than the cap, and the cap was really reached
    }

    [Fact]
    public async Task A_slow_queue_does_not_block_a_fast_queue()
    {
        for (var i = 0; i < 3; i++)
            await Repo.EnqueueJsonAsync("SlowJob", """{"seconds": 6}""", new JobOptions { Queue = "slow" });
        for (var i = 0; i < 10; i++)
            await Repo.EnqueueJsonAsync("ProbeJob", null, new JobOptions { Queue = "fast" });

        using var host = BuildHost(o =>
        {
            o.Queues["slow"] = new QueueOptions { Concurrency = 1, BatchSize = 1 };
            o.Queues["fast"] = new QueueOptions { Concurrency = 4, BatchSize = 4 };
            o.PollInterval = TimeSpan.FromMilliseconds(100);
            o.DrainTimeout = TimeSpan.FromSeconds(1);   // so StopAsync doesn't wait 25s on the slow jobs
        });
        await host.StartAsync();

        await WaitForAsync(
            async () => await CountAsync("SELECT count(*) FROM jobs WHERE queue = 'fast' AND status = 2") == 10,
            TimeSpan.FromSeconds(15), "the fast queue to finish");

        // fast is completely done while the slow queue is still working through its first 6s job
        Assert.Equal(0, await CountAsync("SELECT count(*) FROM jobs WHERE queue = 'slow' AND status = 2"));
        Assert.Equal(3, await CountAsync("SELECT count(*) FROM jobs WHERE queue = 'slow' AND status IN (0, 1)"));

        await host.StopAsync();
    }

    [Fact]
    public async Task Rate_limit_spreads_job_starts_over_time()
    {
        for (var i = 0; i < 20; i++)
            await Repo.EnqueueJsonAsync("TickJob", null);

        using var host = BuildHost(o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(100);
            o.RateLimits["TickJob"] = 10;   // 10 starts per second
        });
        await host.StartAsync();

        await WaitForAsync(
            async () => await CountAsync("SELECT count(*) FROM job_effects WHERE handler = 'TickJob'") == 20,
            TimeSpan.FromSeconds(30), "all 20 ticks to run");
        await host.StopAsync();

        await using var cmd = Db.CreateCommand(
            "SELECT extract(epoch FROM (max(executed_at) - min(executed_at))) FROM job_effects WHERE handler = 'TickJob'");
        var spread = Convert.ToDouble(await cmd.ExecuteScalarAsync());

        Assert.InRange(spread, 1.5, 8.0);   // 20 jobs at 10/s take ~1.9s; unlimited they'd finish within ~0.2s
    }
}