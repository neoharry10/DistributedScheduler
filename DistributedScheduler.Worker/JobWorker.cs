using System.Collections.Concurrent;
using System.Threading.Channels;
using DistributedScheduler.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace DistributedScheduler.Worker;

public class JobWorker(
    IJobRepository repo,
    IServiceScopeFactory scopes,
    IOptions<WorkerOptions> options,
    ILogger<JobWorker> log) : BackgroundService
{
    private readonly WorkerOptions _o = options.Value;
    private readonly string _workerId = options.Value.WorkerId ?? $"{Environment.MachineName}-{Environment.ProcessId}";
    private readonly ConcurrentDictionary<Guid, RunningJob> _running = new();
    private readonly Dictionary<string, RateGate> _gates = new();   // filled once at startup, read-only after
    private volatile bool _drainExpired;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        foreach (var (type, perSecond) in _o.RateLimits) _gates[type] = new RateGate(perSecond);

        var writers = new List<ChannelWriter<Job>>();
        var pollers = new List<Task>();
        var consumers = new List<Task>();

        foreach (var (queue, q) in _o.EffectiveQueues())
        {
            var slots = new SemaphoreSlim(q.Concurrency);
            var channel = Channel.CreateBounded<Job>(q.Concurrency);   // capacity == slots, so writes never block
            writers.Add(channel.Writer);

            for (var i = 0; i < q.Concurrency; i++)
                consumers.Add(Task.Run(() => ConsumeAsync(channel.Reader, slots)));

            pollers.Add(Task.Run(() => PollAsync(queue, q, channel.Writer, slots, stoppingToken)));
            log.LogInformation("Worker {WorkerId} listening on queue '{Queue}' with {Concurrency} slot(s)",
                _workerId, queue, q.Concurrency);
        }

        // Its own token on purpose: jobs keep running while we drain after stoppingToken fires,
        // so their leases must keep being extended until the very end.
        using var heartbeatCts = new CancellationTokenSource();
        var heartbeat = Task.Run(() => HeartbeatAsync(heartbeatCts.Token));

        try
        {
            await Task.WhenAll(pollers);
        }
        finally
        {
            foreach (var w in writers) w.Complete();
            await DrainAsync(Task.WhenAll(consumers));
            heartbeatCts.Cancel();
            await heartbeat;
        }
    }

    // ---------- claiming (one loop per queue) ----------

    private async Task PollAsync(string queue, QueueOptions q, ChannelWriter<Job> writer, SemaphoreSlim slots, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var taken = 0;
            try
            {
                await slots.WaitAsync(ct);
                taken = 1;
                while (taken < q.BatchSize && slots.Wait(0)) taken++;
            }
            catch (OperationCanceledException) { break; }

            IReadOnlyList<Job> jobs = [];
            try
            {
                jobs = await repo.ClaimAsync(_workerId, queue, taken, _o.Lease, ct);
            }
            catch (OperationCanceledException) { /* shutting down */ }
            catch (Exception ex) { log.LogError(ex, "Claim failed on queue '{Queue}', backing off", queue); }

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
        if (_drainExpired) rj.Cancel(CancelReason.Shutdown);

        // Continue the trace that was active when the job was enqueued (a failed parse just starts a new trace).
        ActivityContext.TryParse(job.TraceParent, null, true, out var parent);
        using var activity = Telemetry.Source.StartActivity($"job {job.Type}", ActivityKind.Consumer, parent);
        activity?.SetTag("scheduler.job.id", job.Id);
        activity?.SetTag("scheduler.queue", job.Queue);
        activity?.SetTag("scheduler.attempt", job.Attempts);
        activity?.SetTag("scheduler.worker", _workerId);

        // Clamped at 0: the app clock and the DB clock can differ slightly.
        Telemetry.QueueLatency.Record(Math.Max(0, (DateTime.UtcNow - job.RunAt).TotalSeconds), Tags(job));

        long? started = null;
        Exception? error = null;
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var handler = scope.ServiceProvider.GetServices<IJobHandler>()
                            .FirstOrDefault(h => h.Type == job.Type)
                        ?? throw new InvalidOperationException($"No handler for job type '{job.Type}'");

            if (_gates.TryGetValue(job.Type, out var gate)) await gate.WaitAsync(rj.Token);

            started = Stopwatch.GetTimestamp();   // measure the handler, not the rate-limit wait
            await handler.HandleAsync(job, rj.Token);
        }
        catch (Exception ex) { error = ex; }
        finally
        {
            _running.TryRemove(job.Id, out _);
            rj.Dispose();
        }

        if (started is { } t0)
            Telemetry.Duration.Record(Stopwatch.GetElapsedTime(t0).TotalSeconds, Tags(job, error is null ? "ok" : "error"));

        string outcome;
        if (error is null)
        {
            var completed = await repo.CompleteAsync(job.Id, _workerId);
            if (!completed) log.LogWarning("Job {JobId} finished but we no longer hold the lease", job.Id);
            outcome = completed ? "succeeded" : "lease_lost";
        }
        else
        {
            switch (rj.Reason)
            {
                case CancelReason.Shutdown:
                    var released = await repo.ReleaseAsync(job.Id, _workerId);
                    log.LogInformation("Job {JobId} interrupted by shutdown, released={Released}", job.Id, released);
                    outcome = "released";
                    break;

                case CancelReason.LeaseLost:
                    log.LogWarning("Job {JobId} cancelled because its lease was lost", job.Id);
                    outcome = "lease_lost";
                    break;

                default:
                    var delay = Backoff.Delay(job.Attempts, _o.RetryBase, _o.RetryCap, Random.Shared.NextDouble());
                    var recorded = await repo.FailAsync(job.Id, _workerId, error.ToString(), delay);
                    log.LogWarning("Job {JobId} ({Type}) failed on attempt {Attempt}/{Max}, recorded={Recorded}: {Message}",
                        job.Id, job.Type, job.Attempts, job.MaxAttempts, recorded, error.Message);
                    outcome = job.Attempts >= job.MaxAttempts ? "dead" : "retry";
                    activity?.AddException(error);
                    activity?.SetStatus(ActivityStatusCode.Error, error.Message);
                    break;
            }
        }

        activity?.SetTag("scheduler.outcome", outcome);
        Telemetry.Finished.Add(1, Tags(job, outcome));
    }

    private static TagList Tags(Job job, string? outcome = null)
    {
        var tags = new TagList { { "type", job.Type }, { "queue", job.Queue } };
        if (outcome is not null) tags.Add("outcome", outcome);
        return tags;
    }

    // ---------- heartbeat (shared by all queues) ----------

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

    // ---------- graceful shutdown (one drain for all queues) ----------

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
            _drainExpired = true;
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