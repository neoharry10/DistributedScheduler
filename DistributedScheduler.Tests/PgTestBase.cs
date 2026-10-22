using DistributedScheduler.Data;
using DistributedScheduler.Domain;
using Npgsql;
using Testcontainers.PostgreSql;
using DistributedScheduler.Worker;
using Microsoft.Extensions.Hosting;

namespace DistributedScheduler.Tests;

public abstract class PgTestBase : IAsyncLifetime
{
    protected readonly PostgreSqlContainer Pg = new PostgreSqlBuilder("postgres:16").Build();
    protected NpgsqlDataSource Db = default!;
    protected JobRepository Repo = default!;

    public async Task InitializeAsync()
    {
        await Pg.StartAsync();
        Migrator.Run(Pg.GetConnectionString());
        Db = NpgsqlDataSource.Create(Pg.GetConnectionString());
        Repo = new JobRepository(Db);
    }

    public async Task DisposeAsync()
    {
        await Db.DisposeAsync();
        await Pg.DisposeAsync();
    }

    protected async Task<Guid> EnqueueAsync(string type, string? payload = null, int maxAttempts = 5)
    {
        var id = Guid.NewGuid();
        await Repo.EnqueueAsync(new Job
        {
            Id = id, Type = type, Payload = payload,
            MaxAttempts = maxAttempts, RunAt = DateTime.UtcNow.AddSeconds(-1)
        });
        return id;
    }

    protected async Task<JobStatus> StatusAsync(Guid id) =>
        (JobStatus)(await ScalarAsync<short>("SELECT status FROM jobs WHERE id = @id", id));

    protected Task<int> AttemptsAsync(Guid id) =>
        ScalarAsync<int>("SELECT attempts FROM jobs WHERE id = @id", id);

    protected Task<string?> LockedByAsync(Guid id) =>
        ScalarAsync<string?>("SELECT locked_by FROM jobs WHERE id = @id", id);

    protected Task<long> EffectsAsync(Guid id) =>
        ScalarAsync<long>("SELECT count(*) FROM job_effects WHERE job_id = @id", id);

    private async Task<T> ScalarAsync<T>(string sql, Guid id)
    {
        await using var cmd = Db.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", id);
        var result = await cmd.ExecuteScalarAsync();
        return result is null or DBNull ? default! : (T)result;
    }

    protected static async Task WaitForAsync(
        Func<Task<bool>> condition, TimeSpan timeout, string what, Func<string>? diagnostics = null)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(200);
        }
        throw new TimeoutException($"Timed out waiting for: {what}{Environment.NewLine}{diagnostics?.Invoke()}");
    }

    protected async Task ExecAsync(string sql)
    {
        await using var cmd = Db.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    protected async Task<long> CountAsync(string sql)
    {
        await using var cmd = Db.CreateCommand(sql);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    protected IHost BuildHost(Action<WorkerOptions> configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddJobWorker(Pg.GetConnectionString(), configure);
        return builder.Build();
    }

    protected Task<string?> LastErrorAsync(Guid id) =>
        ScalarAsync<string?>("SELECT last_error FROM jobs WHERE id = @id", id);
}