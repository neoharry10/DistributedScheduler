namespace DistributedScheduler.Domain;

public enum JobStatus : short { Queued = 0, Running = 1, Succeeded = 2, Failed = 3, Dead = 4, Cancelled = 5 }

public record ThroughputPoint(DateTime Minute, long Succeeded, long Dead);

public class Job
{
    public Guid Id { get; set; }
    public string Type { get; set; } = default!;
    public string? Payload { get; set; }          // jsonb in Postgres
    public string Queue { get; set; } = "default";
    public JobStatus Status { get; set; }
    public int Priority { get; set; }
    public DateTime RunAt { get; set; }           // always UTC
    public int Attempts { get; set; }
    public int MaxAttempts { get; set; } = 5;
    public string? LockedBy { get; set; }
    public DateTime? LockedUntil { get; set; }
    public string? LastError { get; set; }
    public string? IdempotencyKey { get; set; }
    public DateTime CreatedAt { get; set; }
    public string? RecurringId { get; set; }
    public DateTime? ScheduledFor { get; set; }
    public string? TraceParent { get; set; }
}