namespace DistributedScheduler.Domain;

public interface IJobHandler
{
    string Type { get; }
    Task HandleAsync(Job job, CancellationToken ct);
}