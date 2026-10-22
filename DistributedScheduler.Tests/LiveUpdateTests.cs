using DistributedScheduler.Domain;
using Npgsql;

namespace DistributedScheduler.Tests;

public class LiveUpdateTests : PgTestBase
{
    [Fact]
    public async Task Status_changes_notify_but_lease_extensions_do_not()
    {
        var received = 0;
        await using var listener = await Db.OpenConnectionAsync();
        listener.Notification += (_, _) => Interlocked.Increment(ref received);
        await using (var listen = new NpgsqlCommand("LISTEN jobs_changed", listener))
            await listen.ExecuteNonQueryAsync();

        using var cts = new CancellationTokenSource();
        var pump = Task.Run(async () =>
        {
            try { while (true) await listener.WaitAsync(cts.Token); }
            catch (OperationCanceledException) { }
        });

        try
        {
            var before = Volatile.Read(ref received);
            var id = await EnqueueAsync("SendEmailJob");
            await WaitForAsync(() => Task.FromResult(Volatile.Read(ref received) > before),
                TimeSpan.FromSeconds(5), "a notification for the insert");

            before = Volatile.Read(ref received);
            await Repo.ClaimAsync("w1", "default", 1, TimeSpan.FromSeconds(30));
            await WaitForAsync(() => Task.FromResult(Volatile.Read(ref received) > before),
                TimeSpan.FromSeconds(5), "a notification for the claim");

            // The heartbeat only touches locked_until, so it must stay silent.
            before = Volatile.Read(ref received);
            await Repo.ExtendLeasesAsync("w1", TimeSpan.FromSeconds(30));
            await Task.Delay(1000);
            Assert.Equal(before, Volatile.Read(ref received));

            before = Volatile.Read(ref received);
            await Repo.CompleteAsync(id, "w1");
            await WaitForAsync(() => Task.FromResult(Volatile.Read(ref received) > before),
                TimeSpan.FromSeconds(5), "a notification for the completion");
        }
        finally
        {
            cts.Cancel();
            await pump;
        }
    }

    [Fact]
    public async Task Finished_at_is_set_when_a_job_finishes_and_cleared_on_requeue()
    {
        var id = await EnqueueAsync("PoisonJob", maxAttempts: 1);
        await Repo.ClaimAsync("w1", "default", 1, TimeSpan.FromSeconds(30));
        Assert.Equal(0, await CountAsync($"SELECT count(*) FROM jobs WHERE id = '{id}' AND finished_at IS NOT NULL"));

        await Repo.FailAsync(id, "w1", "boom", TimeSpan.Zero);   // 1 attempt of 1 -> Dead
        Assert.Equal(JobStatus.Dead, await StatusAsync(id));
        Assert.Equal(1, await CountAsync($"SELECT count(*) FROM jobs WHERE id = '{id}' AND finished_at IS NOT NULL"));

        await Repo.RequeueAsync(id);
        Assert.Equal(0, await CountAsync($"SELECT count(*) FROM jobs WHERE id = '{id}' AND finished_at IS NOT NULL"));
    }

    [Fact]
    public async Task Throughput_counts_succeeded_and_dead_jobs()
    {
        var a = await EnqueueAsync("SendEmailJob", maxAttempts: 1);
        var b = await EnqueueAsync("SendEmailJob", maxAttempts: 1);
        var c = await EnqueueAsync("SendEmailJob", maxAttempts: 1);

        var claimed = await Repo.ClaimAsync("w1", "default", 10, TimeSpan.FromSeconds(30));
        Assert.Equal(3, claimed.Count);

        await Repo.CompleteAsync(a, "w1");
        await Repo.CompleteAsync(b, "w1");
        await Repo.FailAsync(c, "w1", "boom", TimeSpan.Zero);

        var points = await Repo.ThroughputAsync(15);
        Assert.Equal(2, points.Sum(p => p.Succeeded));
        Assert.Equal(1, points.Sum(p => p.Dead));
    }
}