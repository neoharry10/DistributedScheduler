namespace DistributedScheduler.Worker;

public class QueueOptions
{
    public int Concurrency { get; set; } = 8;
    public int BatchSize { get; set; } = 8;
}

public class WorkerOptions
{
    // Single-queue shortcut. Only used when Queues is empty.
    public string Queue { get; set; } = "default";
    public int Concurrency { get; set; } = 8;
    public int BatchSize { get; set; } = 8;

    // Multi-queue setup: queue name -> its own cap. Wins over the three settings above.
    public Dictionary<string, QueueOptions> Queues { get; set; } = new();

    // Job type -> max starts per second, per worker process.
    public Dictionary<string, double> RateLimits { get; set; } = new();

    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan RetryBase { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan RetryCap { get; set; } = TimeSpan.FromMinutes(5);

    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(10);   // rule: <= Lease / 3
    public TimeSpan ReaperInterval { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan SchedulerInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(25);        // must be < host ShutdownTimeout

    public IReadOnlyDictionary<string, QueueOptions> EffectiveQueues() =>
        Queues.Count > 0
            ? Queues
            : new Dictionary<string, QueueOptions> { [Queue] = new() { Concurrency = Concurrency, BatchSize = BatchSize } };
}