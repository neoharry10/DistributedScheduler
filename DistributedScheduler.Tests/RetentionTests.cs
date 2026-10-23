using DistributedScheduler.Domain;

namespace DistributedScheduler.Tests;

public class RetentionTests : PgTestBase
{
    [Fact]
    public async Task Old_finished_jobs_are_deleted_and_everything_else_is_kept()
    {
        var oldOk = await EnqueueAsync("SendEmailJob");
        var newOk = await EnqueueAsync("SendEmailJob");
        var oldDead = await EnqueueAsync("PoisonJob", maxAttempts: 1);

        await Repo.ClaimAsync("w1", "default", 10, TimeSpan.FromSeconds(30));
        await Repo.CompleteAsync(oldOk, "w1");
        await Repo.CompleteAsync(newOk, "w1");
        await Repo.FailAsync(oldDead, "w1", "boom", TimeSpan.Zero);   // 1 attempt of 1 -> Dead
        var queued = await EnqueueAsync("SendEmailJob");              // enqueued after the claim, stays Queued

        // finished_at is only set by the trigger on status changes, so backdating it directly is safe
        await ExecAsync($"UPDATE jobs SET finished_at = now() - interval '8 days' WHERE id IN ('{oldOk}', '{oldDead}')");

        var deleted = await Repo.DeleteFinishedAsync(TimeSpan.FromDays(7), TimeSpan.FromDays(30), 100);

        Assert.Equal(1, deleted);                                     // only the 8-day-old succeeded job
        Assert.Equal(0, await CountAsync($"SELECT count(*) FROM jobs WHERE id = '{oldOk}'"));
        Assert.Equal(3, await CountAsync("SELECT count(*) FROM jobs"));   // newOk, oldDead (8d < 30d) and queued survive

        await ExecAsync($"UPDATE jobs SET finished_at = now() - interval '31 days' WHERE id = '{oldDead}'");
        Assert.Equal(1, await Repo.DeleteFinishedAsync(TimeSpan.FromDays(7), TimeSpan.FromDays(30), 100));
        Assert.Equal(JobStatus.Queued, await StatusAsync(queued));
    }
}