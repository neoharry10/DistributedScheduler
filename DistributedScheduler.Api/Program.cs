using System.Text.Json;
using DistributedScheduler.Data;
using DistributedScheduler.Domain;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

var cs = builder.Configuration.GetConnectionString("Scheduler")
    ?? throw new InvalidOperationException("ConnectionStrings:Scheduler is missing");

Migrator.Run(cs);   // the Api owns the schema; workers just connect, so start the Api first

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(cs));
builder.Services.AddSingleton<IJobRepository, JobRepository>();

var app = builder.Build();

static object ToDto(Job j) => new
{
    j.Id, j.Type, Status = j.Status.ToString(), j.Queue, j.Priority, j.Attempts, j.MaxAttempts,
    j.RunAt, j.LockedBy, j.LockedUntil, j.LastError, j.IdempotencyKey, j.RecurringId, j.CreatedAt,
    Payload = j.Payload is null ? (JsonElement?)null : JsonSerializer.Deserialize<JsonElement>(j.Payload)
};

static string? RawPayload(JsonElement? p) =>
    p is { ValueKind: not (JsonValueKind.Null or JsonValueKind.Undefined) } v ? v.GetRawText() : null;

app.MapPost("/jobs", async (EnqueueRequest req, IJobRepository repo, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(req.Type)) return Results.BadRequest(new { error = "type is required" });

    var options = new JobOptions
    {
        Queue = req.Queue ?? "default",
        Priority = req.Priority ?? 0,
        MaxAttempts = req.MaxAttempts ?? 5,
        RunAt = req.RunAt?.UtcDateTime,
        Delay = req.DelaySeconds is { } s ? TimeSpan.FromSeconds(s) : null,
        IdempotencyKey = req.IdempotencyKey
    };

    var result = await repo.EnqueueJsonAsync(req.Type, RawPayload(req.Payload), options, ct);
    return Results.Accepted($"/jobs/{result.Id}", new { id = result.Id, created = result.Created });
});

app.MapGet("/jobs/{id:guid}", async (Guid id, IJobRepository repo, CancellationToken ct) =>
    await repo.GetAsync(id, ct) is { } job ? Results.Ok(ToDto(job)) : Results.NotFound());

// Dead-letter view = GET /jobs?status=Dead
app.MapGet("/jobs", async (string? status, string? type, int? limit, int? offset, IJobRepository repo, CancellationToken ct) =>
{
    JobStatus? parsed = null;
    if (status is not null)
    {
        if (!Enum.TryParse<JobStatus>(status, ignoreCase: true, out var s))
            return Results.BadRequest(new { error = $"unknown status '{status}'" });
        parsed = s;
    }

    var jobs = await repo.ListAsync(parsed, type, Math.Clamp(limit ?? 50, 1, 200), Math.Max(offset ?? 0, 0), ct);
    return Results.Ok(jobs.Select(ToDto));
});

app.MapGet("/jobs/stats", async (IJobRepository repo, CancellationToken ct) =>
{
    var counts = await repo.CountByStatusAsync(ct);
    return Results.Ok(Enum.GetValues<JobStatus>().ToDictionary(s => s.ToString(), s => counts.GetValueOrDefault(s)));
});

app.MapPost("/jobs/{id:guid}/requeue", async (Guid id, IJobRepository repo, CancellationToken ct) =>
    await repo.RequeueAsync(id, ct)
        ? Results.Ok(new { id, status = "Queued" })
        : Results.Conflict(new { error = "only Dead or Failed jobs can be requeued" }));

app.MapPost("/jobs/{id:guid}/cancel", async (Guid id, IJobRepository repo, CancellationToken ct) =>
    await repo.CancelAsync(id, ct)
        ? Results.Ok(new { id, status = "Cancelled" })
        : Results.Conflict(new { error = "only Queued jobs can be cancelled (it may already be running)" }));

app.MapPut("/recurring/{id}", async (string id, RecurringRequest req, IJobRepository repo, CancellationToken ct) =>
{
    try
    {
        await repo.UpsertRecurringAsync(new RecurringJob
        {
            Id = id,
            Type = req.Type,
            Payload = RawPayload(req.Payload),
            Queue = req.Queue ?? "default",
            Cron = req.Cron,
            TimeZone = req.TimeZone ?? "UTC",
            Enabled = req.Enabled ?? true
        }, ct);
        return Results.NoContent();
    }
    catch (Exception ex) when (ex is ArgumentException or FormatException or TimeZoneNotFoundException or InvalidTimeZoneException)
    {
        return Results.BadRequest(new { error = ex.Message });   // bad cron / unknown time zone
    }
});

app.Run();

record EnqueueRequest(string Type, JsonElement? Payload, string? Queue, int? Priority, int? MaxAttempts,
                      DateTimeOffset? RunAt, int? DelaySeconds, string? IdempotencyKey);

record RecurringRequest(string Type, string Cron, JsonElement? Payload, string? Queue, string? TimeZone, bool? Enabled);