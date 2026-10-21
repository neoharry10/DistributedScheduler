namespace DistributedScheduler.Worker;

public class WorkerOptions
{
    public string Queue { get; set; } = "default";
    public int Concurrency { get; set; } = 8;
    public int BatchSize { get; set; } = 8;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan RetryBase { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan RetryCap { get; set; } = TimeSpan.FromMinutes(5);

    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(10);   // rule: <= Lease / 3
    public TimeSpan ReaperInterval { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(25);        // must be < host ShutdownTimeout
}