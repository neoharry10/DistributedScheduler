using System.Threading.Channels;
using DistributedScheduler.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DistributedScheduler.Worker;

public class JobWorker(
    IJobRepository repo,
    IServiceScopeFactory scopes,
    IOptions<WorkerOptions> options,
    ILogger<JobWorker> log) : BackgroundService
{
    private readonly WorkerOptions _o = options.Value;
    private readonly string _workerId = $"{Environment.MachineName}-{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var slots = new SemaphoreSlim(_o.Concurrency);
        var channel = Channel.CreateBounded<Job>(_o.Concurrency);

        var consumers = Enumerable.Range(0, _o.Concurrency)
            .Select(_ => Task.Run(() => ConsumeAsync(channel.Reader, slots)))
            .ToArray();

        try
        {
            await PollAsync(channel.Writer, slots, stoppingToken);
        }
        finally
        {
            channel.Writer.Complete();          // consumers drain what's left, then exit
            await Task.WhenAll(consumers);      // in-flight jobs finish before we return
        }
    }

    private async Task PollAsync(ChannelWriter<Job> writer, SemaphoreSlim slots, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // Grab as many free slots as we can (up to BatchSize), blocking for at least one.
            var taken = 0;
            try
            {
                await slots.WaitAsync(ct);
                taken = 1;
                while (taken < _o.BatchSize && slots.Wait(0)) taken++;
            }
            catch (OperationCanceledException) { break; }

            IReadOnlyList<Job> jobs = [];
            try
            {
                jobs = await repo.ClaimAsync(_workerId, _o.Queue, taken, _o.Lease, ct);
            }
            catch (OperationCanceledException) { /* shutting down */ }
            catch (Exception ex) { log.LogError(ex, "Claim failed, backing off"); }

            if (taken > jobs.Count) slots.Release(taken - jobs.Count);   // return unused slots

            // Never blocks: channel capacity == total slots.
            foreach (var job in jobs) await writer.WriteAsync(job, CancellationToken.None);

            if (jobs.Count == 0)
            {
                try { await Task.Delay(_o.PollInterval, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task ConsumeAsync(ChannelReader<Job> reader, SemaphoreSlim slots)
    {
        await foreach (var job in reader.ReadAllAsync())
        {
            try { await RunAsync(job); }
            catch (Exception ex) { log.LogError(ex, "Could not record result for job {JobId}", job.Id); }
            finally { slots.Release(); }
        }
    }

    private async Task RunAsync(Job job)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var handler = scope.ServiceProvider.GetServices<IJobHandler>()
                              .FirstOrDefault(h => h.Type == job.Type)
                          ?? throw new InvalidOperationException($"No handler for job type '{job.Type}'");

            // M2: jobs run to completion even on shutdown. M3 adds cancellation + drain timeout.
            await handler.HandleAsync(job, CancellationToken.None);
        }
        catch (Exception ex)
        {
            var delay = Backoff.Delay(job.Attempts, _o.RetryBase, _o.RetryCap, Random.Shared.NextDouble());
            var recorded = await repo.FailAsync(job.Id, _workerId, ex.ToString(), delay);
            log.LogWarning("Job {JobId} ({Type}) failed on attempt {Attempt}/{Max}, recorded={Recorded}: {Message}",
                job.Id, job.Type, job.Attempts, job.MaxAttempts, recorded, ex.Message);
            return;
        }

        if (!await repo.CompleteAsync(job.Id, _workerId))
            log.LogWarning("Job {JobId} finished but we no longer hold the lease", job.Id);
    }
}