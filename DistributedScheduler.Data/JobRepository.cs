using DistributedScheduler.Domain;
using Npgsql;

namespace DistributedScheduler.Data;

public class JobRepository(NpgsqlDataSource db) : IJobRepository
{
    public async Task<bool> EnqueueAsync(Job job, CancellationToken ct = default)
    {
        const string sql = """
            INSERT INTO jobs (id, type, payload, queue, priority, run_at, max_attempts, idempotency_key)
            VALUES (@id, @type, @payload::jsonb, @queue, @priority, @run_at, @max_attempts, @idempotency_key)
            ON CONFLICT (idempotency_key) DO NOTHING;
            """;

        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", job.Id);
        cmd.Parameters.AddWithValue("type", job.Type);
        cmd.Parameters.AddWithValue("payload", (object?)job.Payload ?? DBNull.Value);
        cmd.Parameters.AddWithValue("queue", job.Queue);
        cmd.Parameters.AddWithValue("priority", job.Priority);
        cmd.Parameters.AddWithValue("run_at", job.RunAt);
        cmd.Parameters.AddWithValue("max_attempts", job.MaxAttempts);
        cmd.Parameters.AddWithValue("idempotency_key", (object?)job.IdempotencyKey ?? DBNull.Value);

        return await cmd.ExecuteNonQueryAsync(ct) == 1;   // false = duplicate idempotency key
    }

    public async Task<IReadOnlyList<Job>> ClaimAsync(
        string workerId, string queue, int batchSize, TimeSpan lease, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE jobs
            SET status = 1,
                locked_by = @worker,
                locked_until = now() + make_interval(secs => @lease_seconds),
                attempts = attempts + 1
            WHERE id IN (
                SELECT id FROM jobs
                WHERE status = 0 AND queue = @queue AND run_at <= now()
                ORDER BY priority DESC, run_at
                LIMIT @batch
                FOR UPDATE SKIP LOCKED)
            RETURNING id, type, payload::text, queue, status, priority, run_at,
                      attempts, max_attempts, locked_by, locked_until, last_error,
                      idempotency_key, created_at;
            """;

        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("worker", workerId);
        cmd.Parameters.AddWithValue("queue", queue);
        cmd.Parameters.AddWithValue("batch", batchSize);
        cmd.Parameters.AddWithValue("lease_seconds", lease.TotalSeconds);

        var jobs = new List<Job>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) jobs.Add(Map(r));
        return jobs;
    }

    // Column order must match the RETURNING / SELECT list above.
    private static Job Map(NpgsqlDataReader r) => new()
    {
        Id             = r.GetGuid(0),
        Type           = r.GetString(1),
        Payload        = r.IsDBNull(2)  ? null : r.GetString(2),
        Queue          = r.GetString(3),
        Status         = (JobStatus)r.GetInt16(4),
        Priority       = r.GetInt32(5),
        RunAt          = r.GetDateTime(6),
        Attempts       = r.GetInt32(7),
        MaxAttempts    = r.GetInt32(8),
        LockedBy       = r.IsDBNull(9)  ? null : r.GetString(9),
        LockedUntil    = r.IsDBNull(10) ? null : r.GetDateTime(10),
        LastError      = r.IsDBNull(11) ? null : r.GetString(11),
        IdempotencyKey = r.IsDBNull(12) ? null : r.GetString(12),
        CreatedAt      = r.GetDateTime(13),
    };

    public async Task<bool> CompleteAsync(Guid id, string workerId, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE jobs
            SET status = 2, locked_by = NULL, locked_until = NULL, last_error = NULL
            WHERE id = @id AND status = 1 AND locked_by = @worker;
            """;

        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("worker", workerId);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;   // false = we lost the lease
    }

    public async Task<bool> FailAsync(Guid id, string workerId, string error, TimeSpan retryDelay, CancellationToken ct = default)
    {
        // attempts was already incremented at claim time.
        // Out of attempts -> Dead (4). Otherwise back to Queued (0) with a delayed run_at.
        const string sql = """
            UPDATE jobs
            SET status = CASE WHEN attempts >= max_attempts THEN 4 ELSE 0 END,
                run_at = CASE WHEN attempts >= max_attempts THEN run_at
                            ELSE now() + make_interval(secs => @delay_seconds) END,
                last_error = @error,
                locked_by = NULL,
                locked_until = NULL
            WHERE id = @id AND status = 1 AND locked_by = @worker;
            """;

        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("worker", workerId);
        cmd.Parameters.AddWithValue("error", error);
        cmd.Parameters.AddWithValue("delay_seconds", retryDelay.TotalSeconds);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<IReadOnlyList<Guid>> ExtendLeasesAsync(string workerId, TimeSpan lease, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE jobs
            SET locked_until = now() + make_interval(secs => @lease_seconds)
            WHERE status = 1 AND locked_by = @worker
            RETURNING id;
            """;

        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("worker", workerId);
        cmd.Parameters.AddWithValue("lease_seconds", lease.TotalSeconds);

        var ids = new List<Guid>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) ids.Add(r.GetGuid(0));
        return ids;
    }

    public async Task<bool> ReleaseAsync(Guid id, string workerId, CancellationToken ct = default)
    {
        const string sql = """
            UPDATE jobs
            SET status = 0,
                attempts = GREATEST(attempts - 1, 0),
                run_at = now(),
                locked_by = NULL,
                locked_until = NULL
            WHERE id = @id AND status = 1 AND locked_by = @worker;
            """;

        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("worker", workerId);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<int> ReapExpiredAsync(CancellationToken ct = default)
    {
        // A crash already used up an attempt (counted at claim time).
        // Out of attempts -> Dead, so a job that keeps killing workers eventually gets parked.
        // Several reapers can run at once: the loser blocks on the row lock, re-checks the WHERE, matches nothing.
        const string sql = """
            UPDATE jobs
            SET status = CASE WHEN attempts >= max_attempts THEN 4 ELSE 0 END,
                run_at = now(),
                last_error = 'Lease expired: worker crashed or stalled (held by ' || coalesce(locked_by, '?') || ')',
                locked_by = NULL,
                locked_until = NULL
            WHERE status = 1 AND locked_until < now();
            """;

        await using var cmd = db.CreateCommand(sql);
        return await cmd.ExecuteNonQueryAsync(ct);
    }
}