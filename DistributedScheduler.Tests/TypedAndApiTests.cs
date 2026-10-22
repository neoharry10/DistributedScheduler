using DistributedScheduler.Domain;

namespace DistributedScheduler.Tests;

public class TypedAndApiTests : PgTestBase
{
    [Fact]
    public async Task Typed_payload_round_trips_to_the_handler()
    {
        var ok = await Repo.EnqueueAsync("GenerateInvoiceJob", new InvoicePayload(Guid.NewGuid(), 99.5m));
        var bad = await Repo.EnqueueAsync("GenerateInvoiceJob", new InvoicePayload(Guid.NewGuid(), -5m),
            new JobOptions { MaxAttempts = 2 });

        using var host = BuildHost(o =>
        {
            o.PollInterval = TimeSpan.FromMilliseconds(100);
            o.RetryBase = TimeSpan.FromMilliseconds(50);
        });
        await host.StartAsync();

        await WaitForAsync(async () => await StatusAsync(ok.Id) == JobStatus.Succeeded,
            TimeSpan.FromSeconds(30), "valid invoice to succeed");
        await WaitForAsync(async () => await StatusAsync(bad.Id) == JobStatus.Dead,
            TimeSpan.FromSeconds(30), "invalid invoice to die");
        await host.StopAsync();

        Assert.Equal(1, await EffectsAsync(ok.Id));
        Assert.Equal(0, await EffectsAsync(bad.Id));
        Assert.Contains("Amount must be positive", await LastErrorAsync(bad.Id));
    }

    [Fact]
    public async Task Same_idempotency_key_enqueues_once_even_under_a_race()
    {
        var options = new JobOptions { IdempotencyKey = "invoice-2026-10-1001" };

        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ =>
            Task.Run(() => Repo.EnqueueAsync("GenerateInvoiceJob", new InvoicePayload(Guid.NewGuid(), 10m), options))));

        Assert.Equal(1, results.Count(r => r.Created));                     // exactly one winner
        Assert.Single(results.Select(r => r.Id).Distinct());                // everyone got the same job id back
        Assert.Equal(1, await CountAsync("SELECT count(*) FROM jobs WHERE idempotency_key = 'invoice-2026-10-1001'"));
    }

    [Fact]
    public async Task Dead_job_can_be_requeued_and_queued_job_can_be_cancelled()
    {
        var dead = (await Repo.EnqueueJsonAsync("PoisonJob", null, new JobOptions { MaxAttempts = 1 })).Id;

        using (var host = BuildHost(o => o.PollInterval = TimeSpan.FromMilliseconds(100)))
        {
            await host.StartAsync();
            await WaitForAsync(async () => await StatusAsync(dead) == JobStatus.Dead,
                TimeSpan.FromSeconds(30), "poison job to die");
            await host.StopAsync();
        }

        Assert.True(await Repo.RequeueAsync(dead));
        Assert.Equal(JobStatus.Queued, await StatusAsync(dead));
        Assert.Equal(0, await AttemptsAsync(dead));
        Assert.False(await Repo.RequeueAsync(dead));        // already Queued, nothing to requeue

        var later = (await Repo.EnqueueJsonAsync("SendEmailJob", null,
            new JobOptions { Delay = TimeSpan.FromHours(1) })).Id;
        Assert.True(await Repo.CancelAsync(later));
        Assert.Equal(JobStatus.Cancelled, await StatusAsync(later));
        Assert.False(await Repo.CancelAsync(later));        // can't cancel twice
    }
}