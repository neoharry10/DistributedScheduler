using System.Diagnostics;
using DistributedScheduler.Data;
using DistributedScheduler.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

int Opt(string name, int fallback)
{
    var prefix = $"--{name}=";
    var arg = args.FirstOrDefault(a => a.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));
    return arg is null ? fallback : int.Parse(arg[prefix.Length..]);
}

var jobs = Opt("jobs", 20_000);
var workers = Opt("workers", 2);          // 0 = only seed the table (for EXPLAIN experiments)
var concurrency = Opt("concurrency", 16);
var batch = Opt("batch", 16);
var pollMs = Opt("poll-ms", 50);
var notify = Opt("notify", 1) == 1;       // 0 = drop the NOTIFY triggers from migration 005

var cs = Environment.GetEnvironmentVariable("LOADTEST_CONNECTION")
    ?? "Host=localhost;Port=5432;Database=scheduler_loadtest;Username=scheduler;Password=scheduler";

// A fresh database for every run, so results are comparable. Guarded: this DROPS the database.
var dbName = new NpgsqlConnectionStringBuilder(cs).Database!;
if (!dbName.Contains("loadtest", StringComparison.OrdinalIgnoreCase))
    throw new InvalidOperationException($"Refusing to drop '{dbName}': its name must contain 'loadtest'");

var admin = new NpgsqlConnectionStringBuilder(cs) { Database = "postgres", Pooling = false };
await using (var conn = new NpgsqlConnection(admin.ConnectionString))
{
    await conn.OpenAsync();
    await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{dbName}\" WITH (FORCE)", conn);
    await drop.ExecuteNonQueryAsync();
}
Migrator.Run(cs);

await using var db = NpgsqlDataSource.Create(cs);

async Task<object?> Scalar(string sql) { await using var c = db.CreateCommand(sql); return await c.ExecuteScalarAsync(); }
async Task Exec(string sql) { await using var c = db.CreateCommand(sql); await c.ExecuteNonQueryAsync(); }
async Task<long> Count(string sql) => Convert.ToInt64(await Scalar(sql));

if (!notify)
{
    await Exec("DROP TRIGGER trg_jobs_insert ON jobs");
    await Exec("DROP TRIGGER trg_jobs_status ON jobs");
}

// Seed with binary COPY: fast enough that seeding never dominates the run.
await using (var conn = await db.OpenConnectionAsync())
await using (var writer = await conn.BeginBinaryImportAsync(
    "COPY jobs (id, type, queue, run_at, max_attempts) FROM STDIN (FORMAT BINARY)"))
{
    var runAt = DateTime.UtcNow.AddSeconds(-1);
    for (var i = 0; i < jobs; i++)
    {
        await writer.StartRowAsync();
        await writer.WriteAsync(Guid.NewGuid(), NpgsqlDbType.Uuid);
        await writer.WriteAsync("NoopJob", NpgsqlDbType.Text);
        await writer.WriteAsync("default", NpgsqlDbType.Text);
        await writer.WriteAsync(runAt, NpgsqlDbType.TimestampTz);
        await writer.WriteAsync(5, NpgsqlDbType.Integer);
    }
    await writer.CompleteAsync();
}
await Exec("ANALYZE jobs");

if (workers == 0)
{
    Console.WriteLine($"Seeded {jobs} queued jobs into '{dbName}'. Nothing is running.");
    return;
}

var hosts = new List<IHost>();
for (var i = 0; i < workers; i++)
{
    var workerId = $"load-{i}";   // copy: the options lambda runs later, after the loop variable has moved on
    var builder = Host.CreateApplicationBuilder();
    builder.Logging.SetMinimumLevel(LogLevel.Warning);
    builder.Services.AddJobWorker(cs, o =>
    {
        o.WorkerId = workerId;
        o.Concurrency = concurrency;
        o.BatchSize = batch;
        o.PollInterval = TimeSpan.FromMilliseconds(pollMs);
    });
    hosts.Add(builder.Build());
}

var wall = Stopwatch.StartNew();
foreach (var h in hosts) await h.StartAsync();

var tick = 0;
var failures = 0;
var aborted = false;
long lastRemaining = jobs;
var lastAt = TimeSpan.Zero;

while (true)
{
    long remaining;
    try
    {
        remaining = await Count("SELECT count(*) FROM jobs WHERE status IN (0, 1)");
        failures = 0;
    }
    catch (NpgsqlException ex)
    {
        if (++failures >= 10) { Console.WriteLine($"Database unreachable, aborting: {ex.Message}"); aborted = true; break; }
        await Task.Delay(1000);
        continue;
    }

    if (remaining == 0) break;

    if (++tick % 8 == 0)
    {
        var now = wall.Elapsed;
        var rate = (lastRemaining - remaining) / (now - lastAt).TotalSeconds;
        Console.WriteLine($"  {now.TotalSeconds,5:F1}s  remaining={remaining}  ~{rate:F0} jobs/s");
        lastRemaining = remaining;
        lastAt = now;
    }

    if (wall.Elapsed > TimeSpan.FromMinutes(5)) { Console.WriteLine("Gave up after 5 minutes"); break; }
    await Task.Delay(250);
}
wall.Stop();

foreach (var h in hosts) { await h.StopAsync(); h.Dispose(); }
if (aborted) return;

var succeeded = await Count("SELECT count(*) FROM jobs WHERE status = 2");
var retried = await Count("SELECT count(*) FROM jobs WHERE attempts > 1");
// Measured from the DB's own timestamps (first to last completion): no startup or polling noise.
var span = Convert.ToDouble(await Scalar(
    "SELECT extract(epoch FROM max(finished_at) - min(finished_at)) FROM jobs WHERE status = 2"));

Console.WriteLine(
    $"RESULT jobs={jobs} workers={workers} concurrency={concurrency} batch={batch} notify={(notify ? 1 : 0)} " +
    $"-> {succeeded / span,6:F0} jobs/s  ({span:F1}s; wall {wall.Elapsed.TotalSeconds:F1}s; succeeded={succeeded}; retried={retried})");