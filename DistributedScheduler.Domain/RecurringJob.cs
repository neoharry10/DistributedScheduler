namespace DistributedScheduler.Domain;

public class RecurringJob
{
    public string Id { get; set; } = default!;
    public string Type { get; set; } = default!;
    public string? Payload { get; set; }
    public string Queue { get; set; } = "default";
    public string Cron { get; set; } = default!;      // 5 fields, or 6 with seconds
    public string TimeZone { get; set; } = "UTC";
    public bool Enabled { get; set; } = true;
    public DateTime NextRunAt { get; set; }
}