using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<Guid, RunningJob> _running = new();
    private volatile bool _drainExpired;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var slots = new SemaphoreSlim(_o.Concurrency);
        var channel = Channel.CreateBounded<Job>(_o.Concurrency);

        var consumers = Enumerable.Range(0, _o.Concurrency)
            .Select(_ => Task.Run(() => ConsumeAsync(channel.Reader, slots)))
            .ToArray();

        // Its own token on purpose: jobs keep running while we drain after stoppingToken fires,
        // so their leases must keep being extended until the very end.
        using var heartbeatCts = new CancellationTokenSource();
        var heartbeat = Task.Run(() => HeartbeatAsync(heartbeatCts.Token));

        try
        {
            await PollAsync(channel.Writer, slots, stoppingToken);
        }
        finally
        {
            channel.Writer.Complete();
            await DrainAsync(Task.WhenAll(consumers));
            heartbeatCts.Cancel();
            await heartbeat;
        }
    }

    // ---------- claiming (same as M2) ----------

    private async Task PollAsync(ChannelWriter<Job> writer, SemaphoreSlim slots, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
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

            if (taken > jobs.Count) slots.Release(taken - jobs.Count);

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

    // ---------- running one job ----------

    private async Task RunAsync(Job job)
    {
        var rj = new RunningJob();
        _running[job.Id] = rj;
        if (_drainExpired) rj.Cancel(CancelReason.Shutdown);   // started after the drain timeout already hit

        Exception? error = null;
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var handler = scope.ServiceProvider.GetServices<IJobHandler>()
                              .FirstOrDefault(h => h.Type == job.Type)
                          ?? throw new InvalidOperationException($"No handler for job type '{job.Type}'");

            await handler.HandleAsync(job, rj.Token);
        }
        catch (Exception ex) { error = ex; }
        finally
        {
            // Deregister BEFORE touching the DB. The heartbeat only cancels jobs that are still registered,
            // so it can never cancel a job that already finished.
            _running.TryRemove(job.Id, out _);
            rj.Dispose();
        }

        if (error is null)
        {
            if (!await repo.CompleteAsync(job.Id, _workerId))
                log.LogWarning("Job {JobId} finished but we no longer hold the lease", job.Id);
            return;
        }

        switch (rj.Reason)
        {
            case CancelReason.Shutdown:
                var released = await repo.ReleaseAsync(job.Id, _workerId);
                log.LogInformation("Job {JobId} interrupted by shutdown, released={Released}", job.Id, released);
                break;

            case CancelReason.LeaseLost:
                // Someone else owns it now. Writing a result here would be wrong (and the guarded SQL would refuse anyway).
                log.LogWarning("Job {JobId} cancelled because its lease was lost", job.Id);
                break;

            default:
                var delay = Backoff.Delay(job.Attempts, _o.RetryBase, _o.RetryCap, Random.Shared.NextDouble());
                var recorded = await repo.FailAsync(job.Id, _workerId, error.ToString(), delay);
                log.LogWarning("Job {JobId} ({Type}) failed on attempt {Attempt}/{Max}, recorded={Recorded}: {Message}",
                    job.Id, job.Type, job.Attempts, job.MaxAttempts, recorded, error.Message);
                break;
        }
    }

    // ---------- heartbeat ----------

    private async Task HeartbeatAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_o.HeartbeatInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try { await BeatAsync(ct); }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    log.LogError(ex, "Heartbeat failed, will retry next tick");
                }
            }
        }
        catch (OperationCanceledException) { /* worker is done */ }
    }

    private async Task BeatAsync(CancellationToken ct)
    {
        // Snapshot BEFORE the query: everything in it was already claimed, so if the DB says we
        // don't hold it anymore, the lease is genuinely lost (not just "claimed a moment ago").
        var snapshot = _running.Keys.ToArray();
        var held = (await repo.ExtendLeasesAsync(_workerId, _o.Lease, ct)).ToHashSet();

        foreach (var id in snapshot)
        {
            if (!held.Contains(id) && _running.TryGetValue(id, out var rj))
            {
                log.LogWarning("Lease lost for job {JobId}, cancelling it", id);
                rj.Cancel(CancelReason.LeaseLost);
            }
        }
    }

    // ---------- graceful shutdown ----------

    private async Task DrainAsync(Task consumers)
    {
        log.LogInformation("Draining: waiting up to {Timeout} for {Count} running job(s)", _o.DrainTimeout, _running.Count);
        try
        {
            await consumers.WaitAsync(_o.DrainTimeout);
        }
        catch (TimeoutException)
        {
            log.LogWarning("Drain timeout: cancelling {Count} running job(s)", _running.Count);
            _drainExpired = true;   // set the flag first so a job that starts right now cancels itself
            foreach (var rj in _running.Values) rj.Cancel(CancelReason.Shutdown);
            await consumers;
        }
    }
}

internal enum CancelReason { None = 0, LeaseLost = 1, Shutdown = 2 }

internal sealed class RunningJob : IDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private int _reason;

    public CancellationToken Token => _cts.Token;
    public CancelReason Reason => (CancelReason)Volatile.Read(ref _reason);

    public void Cancel(CancelReason reason)
    {
        if (Interlocked.CompareExchange(ref _reason, (int)reason, 0) != 0) return;   // first reason wins
        try { _cts.Cancel(); } catch (ObjectDisposedException) { }
    }

    public void Dispose() => _cts.Dispose();
}