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
                idempotency_key, created_at, recurring_id, scheduled_for;
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
        RecurringId    = r.IsDBNull(14) ? null : r.GetString(14),
        ScheduledFor   = r.IsDBNull(15) ? null : r.GetDateTime(15),
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

    public async Task UpsertRecurringAsync(RecurringJob job, CancellationToken ct = default)
    {
        // A bad cron fails here, at registration, not at 3am.
        var next = CronSchedule.NextAfter(job.Cron, job.TimeZone, DateTime.UtcNow)
            ?? throw new ArgumentException($"Cron '{job.Cron}' never fires", nameof(job));

        // next_run_at only resets when the schedule itself changed, so re-registering on every
        // startup doesn't push the next tick back. (SET expressions all see the OLD row.)
        const string sql = """
            INSERT INTO recurring_jobs (id, type, payload, queue, cron, time_zone, enabled, next_run_at)
            VALUES (@id, @type, @payload::jsonb, @queue, @cron, @time_zone, @enabled, @next_run_at)
            ON CONFLICT (id) DO UPDATE SET
                type = EXCLUDED.type,
                payload = EXCLUDED.payload,
                queue = EXCLUDED.queue,
                enabled = EXCLUDED.enabled,
                next_run_at = CASE WHEN recurring_jobs.cron IS DISTINCT FROM EXCLUDED.cron
                                    OR recurring_jobs.time_zone IS DISTINCT FROM EXCLUDED.time_zone
                                THEN EXCLUDED.next_run_at
                                ELSE recurring_jobs.next_run_at END,
                cron = EXCLUDED.cron,
                time_zone = EXCLUDED.time_zone;
            """;

        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", job.Id);
        cmd.Parameters.AddWithValue("type", job.Type);
        cmd.Parameters.AddWithValue("payload", (object?)job.Payload ?? DBNull.Value);
        cmd.Parameters.AddWithValue("queue", job.Queue);
        cmd.Parameters.AddWithValue("cron", job.Cron);
        cmd.Parameters.AddWithValue("time_zone", job.TimeZone);
        cmd.Parameters.AddWithValue("enabled", job.Enabled);
        cmd.Parameters.AddWithValue("next_run_at", next);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<int> EnqueueDueRecurringAsync(int batchSize = 50, CancellationToken ct = default)
    {
        await using var conn = await db.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Lock the due rows. Other nodes skip whatever we hold. now() is the DB clock, so no app/DB skew.
        const string selectSql = """
            SELECT id, type, payload::text, queue, cron, time_zone, next_run_at, now()
            FROM recurring_jobs
            WHERE enabled AND next_run_at <= now()
            ORDER BY next_run_at
            LIMIT @batch
            FOR UPDATE SKIP LOCKED;
            """;

        var due = new List<(RecurringJob Job, DateTime DbNow)>();
        await using (var cmd = new NpgsqlCommand(selectSql, conn, tx))
        {
            cmd.Parameters.AddWithValue("batch", batchSize);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
                due.Add((new RecurringJob
                {
                    Id = r.GetString(0),
                    Type = r.GetString(1),
                    Payload = r.IsDBNull(2) ? null : r.GetString(2),
                    Queue = r.GetString(3),
                    Cron = r.GetString(4),
                    TimeZone = r.GetString(5),
                    NextRunAt = r.GetDateTime(6)
                }, r.GetDateTime(7)));
        }   // reader closed before we reuse the connection

        var enqueued = 0;
        foreach (var (rj, now) in due)
        {
            // Missed slots are skipped on purpose: after downtime you get ONE catch-up run, not 60.
            var next = CronSchedule.NextAfter(rj.Cron, rj.TimeZone, now);

            await using (var ins = new NpgsqlCommand("""
                INSERT INTO jobs (id, type, payload, queue, run_at, recurring_id, scheduled_for)
                VALUES (@id, @type, @payload::jsonb, @queue, @slot, @rid, @slot)
                ON CONFLICT (recurring_id, scheduled_for) DO NOTHING;
                """, conn, tx))
            {
                ins.Parameters.AddWithValue("id", Guid.NewGuid());
                ins.Parameters.AddWithValue("type", rj.Type);
                ins.Parameters.AddWithValue("payload", (object?)rj.Payload ?? DBNull.Value);
                ins.Parameters.AddWithValue("queue", rj.Queue);
                ins.Parameters.AddWithValue("slot", rj.NextRunAt);
                ins.Parameters.AddWithValue("rid", rj.Id);
                enqueued += await ins.ExecuteNonQueryAsync(ct);
            }

            await using (var upd = new NpgsqlCommand(
                "UPDATE recurring_jobs SET next_run_at = @next, enabled = @enabled WHERE id = @id;", conn, tx))
            {
                upd.Parameters.AddWithValue("next", next ?? rj.NextRunAt);
                upd.Parameters.AddWithValue("enabled", next is not null);   // never fires again -> disable
                upd.Parameters.AddWithValue("id", rj.Id);
                await upd.ExecuteNonQueryAsync(ct);
            }
        }

        await tx.CommitAsync(ct);
        return enqueued;
    }

    private const string Cols =
        "id, type, payload::text, queue, status, priority, run_at, attempts, max_attempts, " +
        "locked_by, locked_until, last_error, idempotency_key, created_at, recurring_id, scheduled_for";

    public async Task<Job?> GetAsync(Guid id, CancellationToken ct = default)
    {
        await using var cmd = db.CreateCommand($"SELECT {Cols} FROM jobs WHERE id = @id");
        cmd.Parameters.AddWithValue("id", id);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct) ? Map(r) : null;
    }

    public async Task<Job?> GetByIdempotencyKeyAsync(string key, CancellationToken ct = default)
    {
        await using var cmd = db.CreateCommand($"SELECT {Cols} FROM jobs WHERE idempotency_key = @key");
        cmd.Parameters.AddWithValue("key", key);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct) ? Map(r) : null;
    }

    public async Task<IReadOnlyList<Job>> ListAsync(
        JobStatus? status, string? type, int limit, int offset, CancellationToken ct = default)
    {
        var sql = $"""
            SELECT {Cols} FROM jobs
            WHERE (@status::smallint IS NULL OR status = @status::smallint)
            AND (@type::text IS NULL OR type = @type::text)
            ORDER BY created_at DESC, id
            LIMIT @limit OFFSET @offset;
            """;

        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("status", status is null ? DBNull.Value : (object)(short)status.Value);
        cmd.Parameters.AddWithValue("type", (object?)type ?? DBNull.Value);
        cmd.Parameters.AddWithValue("limit", limit);
        cmd.Parameters.AddWithValue("offset", offset);

        var jobs = new List<Job>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) jobs.Add(Map(r));
        return jobs;
    }

    public async Task<IReadOnlyDictionary<JobStatus, long>> CountByStatusAsync(CancellationToken ct = default)
    {
        await using var cmd = db.CreateCommand("SELECT status, count(*) FROM jobs GROUP BY status");
        var result = new Dictionary<JobStatus, long>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) result[(JobStatus)r.GetInt16(0)] = r.GetInt64(1);
        return result;
    }

    public async Task<bool> RequeueAsync(Guid id, CancellationToken ct = default)
    {
        // last_error is kept on purpose: it's the history of why it died, until the next result overwrites it.
        const string sql = """
            UPDATE jobs
            SET status = 0, attempts = 0, run_at = now(), locked_by = NULL, locked_until = NULL
            WHERE id = @id AND status IN (3, 4);
            """;
        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", id);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<bool> CancelAsync(Guid id, CancellationToken ct = default)
    {
        // Only jobs nobody has claimed yet. The status = 0 check is what makes this race-safe against workers.
        const string sql = "UPDATE jobs SET status = 5 WHERE id = @id AND status = 0;";
        await using var cmd = db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", id);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }
}