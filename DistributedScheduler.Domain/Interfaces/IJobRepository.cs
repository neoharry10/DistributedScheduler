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

    /// Create or update a recurring job. Safe to call on every startup.
    Task UpsertRecurringAsync(RecurringJob job, CancellationToken ct = default);

    /// Enqueue one job per due recurring slot and advance the schedule. Returns how many jobs this call enqueued.
    Task<int> EnqueueDueRecurringAsync(int batchSize = 50, CancellationToken ct = default);
}