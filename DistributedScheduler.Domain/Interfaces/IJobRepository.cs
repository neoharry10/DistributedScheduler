namespace DistributedScheduler.Domain;

public interface IJobRepository
{
    Task<bool> EnqueueAsync(Job job, CancellationToken ct = default);
    Task<IReadOnlyList<Job>> ClaimAsync(string workerId, string queue, int batchSize, TimeSpan lease, CancellationToken ct = default);
    Task<bool> CompleteAsync(Guid id, string workerId, CancellationToken ct = default);
    Task<bool> FailAsync(Guid id, string workerId, string error, TimeSpan retryDelay, CancellationToken ct = default);
    Task<IReadOnlyList<Guid>> ExtendLeasesAsync(string workerId, TimeSpan lease, CancellationToken ct = default);
    Task<bool> ReleaseAsync(Guid id, string workerId, CancellationToken ct = default);
    Task<int> ReapExpiredAsync(CancellationToken ct = default);
    Task UpsertRecurringAsync(RecurringJob job, CancellationToken ct = default);
    Task<int> EnqueueDueRecurringAsync(int batchSize = 50, CancellationToken ct = default);

    // reading and managing jobs
    Task<Job?> GetAsync(Guid id, CancellationToken ct = default);
    Task<Job?> GetByIdempotencyKeyAsync(string key, CancellationToken ct = default);
    Task<IReadOnlyList<Job>> ListAsync(JobStatus? status, string? type, int limit, int offset, CancellationToken ct = default);
    Task<IReadOnlyDictionary<JobStatus, long>> CountByStatusAsync(CancellationToken ct = default);
    Task<bool> RequeueAsync(Guid id, CancellationToken ct = default);   // Dead/Failed -> Queued
    Task<bool> CancelAsync(Guid id, CancellationToken ct = default);    // Queued -> Cancelled

    Task<IReadOnlyList<ThroughputPoint>> ThroughputAsync(int minutes, CancellationToken ct = default);
    Task<int> DeleteFinishedAsync(TimeSpan succeededAge, TimeSpan deadAge, int batchSize, CancellationToken ct = default);
}