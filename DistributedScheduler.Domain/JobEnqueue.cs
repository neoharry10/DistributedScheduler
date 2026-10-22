using System.Text.Json;

namespace DistributedScheduler.Domain;

public record JobOptions
{
    public string Queue { get; init; } = "default";
    public int Priority { get; init; }
    public int MaxAttempts { get; init; } = 5;
    public DateTime? RunAt { get; init; }          // UTC
    public TimeSpan? Delay { get; init; }          // alternative to RunAt
    public string? IdempotencyKey { get; init; }
}

public readonly record struct EnqueueResult(Guid Id, bool Created);   // Created=false -> key already existed

public static class JobRepositoryExtensions
{
    public static Task<EnqueueResult> EnqueueAsync<TPayload>(
        this IJobRepository repo, string type, TPayload payload, JobOptions? options = null, CancellationToken ct = default)
        => repo.EnqueueJsonAsync(type, JsonSerializer.Serialize(payload, JobJson.Options), options, ct);

    public static async Task<EnqueueResult> EnqueueJsonAsync(
        this IJobRepository repo, string type, string? payloadJson, JobOptions? options = null, CancellationToken ct = default)
    {
        var o = options ?? new JobOptions();
        var job = new Job
        {
            Id = Guid.NewGuid(),
            Type = type,
            Payload = payloadJson,
            Queue = o.Queue,
            Priority = o.Priority,
            MaxAttempts = o.MaxAttempts,
            RunAt = o.RunAt ?? DateTime.UtcNow + (o.Delay ?? TimeSpan.Zero),
            IdempotencyKey = o.IdempotencyKey
        };

        if (await repo.EnqueueAsync(job, ct)) return new EnqueueResult(job.Id, true);

        // Rejected by the unique key: somebody already enqueued it. Hand back their id.
        var existing = o.IdempotencyKey is null ? null : await repo.GetByIdempotencyKeyAsync(o.IdempotencyKey, ct);
        return existing is null
            ? throw new InvalidOperationException("Enqueue was rejected but no existing job was found")
            : new EnqueueResult(existing.Id, false);
    }
}