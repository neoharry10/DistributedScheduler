namespace DistributedScheduler.Domain;

public interface IJobRepository
{
    Task<bool> EnqueueAsync(Job job, CancellationToken ct = default);
    Task<IReadOnlyList<Job>> ClaimAsync(string workerId, string queue, int batchSize, TimeSpan lease, CancellationToken ct = default);
    Task<bool> CompleteAsync(Guid id, string workerId, CancellationToken ct = default);
    Task<bool> FailAsync(Guid id, string workerId, string error, TimeSpan retryDelay, CancellationToken ct = default);
}