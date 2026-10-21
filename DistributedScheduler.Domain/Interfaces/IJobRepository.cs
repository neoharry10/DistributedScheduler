namespace DistributedScheduler.Domain;

public interface IJobRepository
{
    Task<bool> EnqueueAsync(Job job, CancellationToken ct = default);
    Task<IReadOnlyList<Job>> ClaimAsync(string workerId, string queue, int batchSize, TimeSpan lease, CancellationToken ct = default);
    Task<bool> CompleteAsync(Guid id, string workerId, CancellationToken ct = default);
    Task<bool> FailAsync(Guid id, string workerId, string error, TimeSpan retryDelay, CancellationToken ct = default);

    /// Extends the lease on every running job this worker holds. Returns the ids it still holds.
    Task<IReadOnlyList<Guid>> ExtendLeasesAsync(string workerId, TimeSpan lease, CancellationToken ct = default);

    /// Graceful shutdown: put the job back in the queue and refund the attempt.
    Task<bool> ReleaseAsync(Guid id, string workerId, CancellationToken ct = default);

    /// Requeue (or bury) jobs whose lease expired. Returns how many were touched.
    Task<int> ReapExpiredAsync(CancellationToken ct = default);
}