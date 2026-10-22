using System.Text.Json;
using DistributedScheduler.Domain;
using Npgsql;

namespace DistributedScheduler.Worker;

internal static class Effects
{
    // The "proof" table: one row per successful execution.
    public static async Task RecordAsync(NpgsqlDataSource db, Job job, string handler, CancellationToken ct)
    {
        await using var cmd = db.CreateCommand("INSERT INTO job_effects (job_id, handler) VALUES (@id, @h)");
        cmd.Parameters.AddWithValue("id", job.Id);
        cmd.Parameters.AddWithValue("h", handler);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

public class SendEmailJob(NpgsqlDataSource db) : IJobHandler
{
    public string Type => nameof(SendEmailJob);

    public async Task HandleAsync(Job job, CancellationToken ct)
    {
        await Task.Delay(Random.Shared.Next(200, 800), ct);   // fake SMTP
        await Effects.RecordAsync(db, job, Type, ct);
    }
}

public class FlakyJob(NpgsqlDataSource db) : IJobHandler
{
    public string Type => nameof(FlakyJob);

    public async Task HandleAsync(Job job, CancellationToken ct)
    {
        if (Random.Shared.NextDouble() < 0.5) throw new InvalidOperationException("Flaky failure");
        await Effects.RecordAsync(db, job, Type, ct);
    }
}

public class PoisonJob : IJobHandler
{
    public string Type => nameof(PoisonJob);
    public Task HandleAsync(Job job, CancellationToken ct) => throw new InvalidOperationException("Always fails");
}

public class SlowJob(NpgsqlDataSource db) : IJobHandler
{
    public string Type => nameof(SlowJob);

    public async Task HandleAsync(Job job, CancellationToken ct)
    {
        var seconds = 10;
        if (job.Payload is not null)
        {
            using var doc = JsonDocument.Parse(job.Payload);
            if (doc.RootElement.TryGetProperty("seconds", out var s)) seconds = s.GetInt32();
        }

        await Task.Delay(TimeSpan.FromSeconds(seconds), ct);   // cancelled on shutdown or lost lease
        await Effects.RecordAsync(db, job, Type, ct);          // only reached if the job really finished
    }
}

public class TickJob(NpgsqlDataSource db) : IJobHandler
{
    public string Type => nameof(TickJob);
    public Task HandleAsync(Job job, CancellationToken ct) => Effects.RecordAsync(db, job, Type, ct);
}

public class GenerateInvoiceJob(NpgsqlDataSource db) : JobHandler<InvoicePayload>
{
    protected override async Task HandleAsync(InvoicePayload p, Job job, CancellationToken ct)
    {
        if (p.Amount <= 0) throw new ArgumentException("Amount must be positive");   // fails -> retries -> Dead
        await Task.Delay(300, ct);                                                   // fake PDF rendering
        await Effects.RecordAsync(db, job, Type, ct);
    }
}

public static class ConcurrencyProbe
{
    private static readonly object L = new();
    private static readonly Dictionary<string, int> Cur = new(), Max = new();

    public static void Enter(string key)
    {
        lock (L)
        {
            Cur[key] = Cur.GetValueOrDefault(key) + 1;
            Max[key] = Math.Max(Max.GetValueOrDefault(key), Cur[key]);
        }
    }

    public static void Exit(string key) { lock (L) Cur[key]--; }

    public static int MaxSeen(string key) { lock (L) return Max.GetValueOrDefault(key); }
}

public class ProbeJob(NpgsqlDataSource db) : IJobHandler
{
    public string Type => nameof(ProbeJob);

    public async Task HandleAsync(Job job, CancellationToken ct)
    {
        ConcurrencyProbe.Enter(job.Queue);
        try { await Task.Delay(300, ct); }
        finally { ConcurrencyProbe.Exit(job.Queue); }
        await Effects.RecordAsync(db, job, Type, ct);
    }
}