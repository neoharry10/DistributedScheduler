using DistributedScheduler.Domain;
using DistributedScheduler.Worker;
using Microsoft.Extensions.Hosting;

namespace DistributedScheduler.Tests;

public class RecoveryTests : PgTestBase
{
    // Env-var versions of the same options, for the child processes.
    private static readonly (string, string)[] Fast =
    [
        ("Worker__Lease", "00:00:04"),
        ("Worker__HeartbeatInterval", "00:00:01"),
        ("Worker__ReaperInterval", "00:00:01"),
        ("Worker__PollInterval", "00:00:00.200"),
    ];

    [Fact]
    public async Task Killed_worker_job_is_recovered_by_another_worker()
    {
        var id = await EnqueueAsync("SlowJob", """{"seconds": 10}""");

        using var a = WorkerProcess.Start(Pg.GetConnectionString(), Fast);
        await WaitForAsync(async () => await StatusAsync(id) == JobStatus.Running,
            TimeSpan.FromSeconds(30), "worker A to claim the job", () => a.Output);
        Assert.Equal(a.WorkerId, await LockedByAsync(id));

        a.Kill();   // dies mid-job, can't release anything

        // Nobody is working on it, but the DB doesn't know yet: it stays Running until the lease expires.
        Assert.Equal(JobStatus.Running, await StatusAsync(id));

        using var b = WorkerProcess.Start(Pg.GetConnectionString(), Fast);
        await WaitForAsync(async () => await StatusAsync(id) == JobStatus.Succeeded,
            TimeSpan.FromSeconds(60), "worker B to recover and finish the job", () => b.Output);

        Assert.Equal(2, await AttemptsAsync(id));   // A's attempt + B's attempt
        Assert.Equal(1, await EffectsAsync(id));    // the side effect happened exactly once
    }

    [Fact]
    public async Task Heartbeat_keeps_a_long_job_alive_past_its_lease()
    {
        var id = await EnqueueAsync("SlowJob", """{"seconds": 8}""");   // 8s job, 3s lease

        using var host = BuildHost(o =>
        {
            o.Lease = TimeSpan.FromSeconds(3);
            o.HeartbeatInterval = TimeSpan.FromSeconds(1);
            o.ReaperInterval = TimeSpan.FromMilliseconds(500);
            o.PollInterval = TimeSpan.FromMilliseconds(100);
        });
        await host.StartAsync();

        await WaitForAsync(async () => await StatusAsync(id) == JobStatus.Succeeded,
            TimeSpan.FromSeconds(30), "the long job to succeed");
        await host.StopAsync();

        Assert.Equal(1, await AttemptsAsync(id));   // the reaper never stole it
        Assert.Equal(1, await EffectsAsync(id));    // and it didn't run twice
    }

    [Fact]
    public async Task Shutdown_cancels_job_after_drain_timeout_and_refunds_the_attempt()
    {
        var id = await EnqueueAsync("SlowJob", """{"seconds": 60}""");

        using var host = BuildHost(o =>
        {
            o.DrainTimeout = TimeSpan.FromSeconds(1);
            o.PollInterval = TimeSpan.FromMilliseconds(100);
        });
        await host.StartAsync();
        await WaitForAsync(async () => await StatusAsync(id) == JobStatus.Running,
            TimeSpan.FromSeconds(30), "the job to start");

        await host.StopAsync();   // drain 1s, then cancel the job, release it

        Assert.Equal(JobStatus.Queued, await StatusAsync(id));
        Assert.Equal(0, await AttemptsAsync(id));   // shutdown isn't the job's fault
        Assert.Null(await LockedByAsync(id));
        Assert.Equal(0, await EffectsAsync(id));
    }

    private IHost BuildHost(Action<WorkerOptions> configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddJobWorker(Pg.GetConnectionString(), configure);
        return builder.Build();
    }
}