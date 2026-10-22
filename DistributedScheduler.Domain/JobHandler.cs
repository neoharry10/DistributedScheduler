using System.Text.Json;

namespace DistributedScheduler.Domain;

public static class JobJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}

public abstract class JobHandler<TPayload> : IJobHandler
{
    public virtual string Type => GetType().Name;

    Task IJobHandler.HandleAsync(Job job, CancellationToken ct)
    {
        if (job.Payload is null)
            throw new InvalidOperationException($"Job {job.Id} ({Type}) has no payload");

        var payload = JsonSerializer.Deserialize<TPayload>(job.Payload, JobJson.Options)
            ?? throw new InvalidOperationException($"Job {job.Id} ({Type}) payload deserialized to null");

        return HandleAsync(payload, job, ct);
    }

    protected abstract Task HandleAsync(TPayload payload, Job job, CancellationToken ct);
}