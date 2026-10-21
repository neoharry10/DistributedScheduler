using DistributedScheduler.Domain;

namespace DistributedScheduler.Tests;

public class RecurringTests : PgTestBase
{
    [Fact]
    public async Task Delayed_job_waits_for_its_run_at()
    {
        var id = Guid.NewGuid();
        await Repo.EnqueueAsync(new Job
        {
            Id = id, Type = "SendEmailJob", RunAt = DateTime.UtcNow.AddSeconds(6)
        });

        using var host = BuildHost(o => o.PollInterval = TimeSpan.FromMilliseconds(100));
        await host.StartAsync();

        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(JobStatus.Queued, await StatusAsync(id));      // worker is polling, job is not due yet

        await WaitForAsync(async () => await StatusAsync(id) == JobStatus.Succeeded,
            TimeSpan.FromSeconds(30), "the delayed job to run after its run_at");
        await host.StopAsync();
    }

    [Fact]
    public async Task Concurrent_tickers_enqueue_a_due_slot_exactly_once()
    {
        await Repo.UpsertRecurringAsync(new RecurringJob { Id = "daily", Type = "TickJob", Cron = "0 3 * * *" });
        await ExecAsync("UPDATE recurring_jobs SET next_run_at = now() - interval '1 minute' WHERE id = 'daily'");

        // 16 "nodes" hit the same due slot at the same moment
        var results = await Task.WhenAll(
            Enumerable.Range(0, 16).Select(_ => Task.Run(() => Repo.EnqueueDueRecurringAsync())));

        Assert.Equal(1, results.Sum());
        Assert.Equal(1, await CountAsync("SELECT count(*) FROM jobs WHERE recurring_id = 'daily'"));
        Assert.Equal(1, await CountAsync("SELECT count(*) FROM recurring_jobs WHERE id = 'daily' AND next_run_at > now()"));
    }

    [Fact]
    public async Task Three_nodes_run_every_tick_exactly_once()
    {
        // Every 2 seconds, so the test takes ~30s instead of 10 minutes. Same logic.
        await Repo.UpsertRecurringAsync(new RecurringJob { Id = "tick", Type = "TickJob", Cron = "*/2 * * * * *" });

        (string, string)[] fast =
        [
            ("Worker__SchedulerInterval", "00:00:00.200"),
            ("Worker__PollInterval", "00:00:00.200"),
        ];
        using var n1 = WorkerProcess.Start(Pg.GetConnectionString(), fast);
        using var n2 = WorkerProcess.Start(Pg.GetConnectionString(), fast);
        using var n3 = WorkerProcess.Start(Pg.GetConnectionString(), fast);

        await WaitForAsync(
            async () => await CountAsync("SELECT count(*) FROM job_effects WHERE handler = 'TickJob'") >= 10,
            TimeSpan.FromSeconds(90), "10 ticks to execute",
            () => $"--- n1 ---\n{n1.Output}\n--- n2 ---\n{n2.Output}\n--- n3 ---\n{n3.Output}");

        n1.Kill(); n2.Kill(); n3.Kill();

        // 1) every slot was enqueued, none skipped, none doubled
        var slots = new List<DateTime>();
        await using (var cmd = Db.CreateCommand(
            "SELECT scheduled_for FROM jobs WHERE recurring_id = 'tick' ORDER BY scheduled_for"))
        await using (var r = await cmd.ExecuteReaderAsync())
            while (await r.ReadAsync()) slots.Add(r.GetDateTime(0));

        Assert.True(slots.Count >= 10);
        for (var i = 1; i < slots.Count; i++)
            Assert.Equal(TimeSpan.FromSeconds(2), slots[i] - slots[i - 1]);

        // 2) no job executed its side effect twice
        Assert.Equal(0, await CountAsync(
            "SELECT count(*) FROM (SELECT job_id FROM job_effects GROUP BY job_id HAVING count(*) > 1) x"));
    }
}