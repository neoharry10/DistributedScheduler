using DistributedScheduler.Data;
using DistributedScheduler.Domain;
using DistributedScheduler.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Testcontainers.PostgreSql;

namespace DistributedScheduler.Tests;

public class WorkerTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _pg = new PostgreSqlBuilder("postgres:16").Build();
    private NpgsqlDataSource _db = default!;
    private JobRepository _repo = default!;

    public async Task InitializeAsync()
    {
        await _pg.StartAsync();
        Migrator.Run(_pg.GetConnectionString());
        _db = NpgsqlDataSource.Create(_pg.GetConnectionString());
        _repo = new JobRepository(_db);
    }

    public async Task DisposeAsync()
    {
        await _db.DisposeAsync();
        await _pg.DisposeAsync();
    }

    [Fact]
    public async Task Jobs_succeed_retry_and_die_correctly()
    {
        await EnqueueMany(nameof(SendEmailJob), 100, maxAttempts: 5);
        await EnqueueMany(nameof(FlakyJob), 50, maxAttempts: 5);
        await EnqueueMany(nameof(PoisonJob), 5, maxAttempts: 3);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddJobWorker(_pg.GetConnectionString(), o =>
        {
            o.Concurrency = 8;
            o.BatchSize = 8;
            o.PollInterval = TimeSpan.FromMilliseconds(100);
            o.RetryBase = TimeSpan.FromMilliseconds(50);
            o.RetryCap = TimeSpan.FromMilliseconds(500);
        });

        using var host = builder.Build();
        await host.StartAsync();
        await WaitUntilIdleAsync(TimeSpan.FromSeconds(60));
        await host.StopAsync();

        // emails: all succeeded
        Assert.Equal(100, await Scalar("SELECT count(*) FROM jobs WHERE type='SendEmailJob' AND status=2"));

        // flaky: every one ended Succeeded or Dead, and retries really happened
        Assert.Equal(0, await Scalar("SELECT count(*) FROM jobs WHERE type='FlakyJob' AND status NOT IN (2,4)"));
        Assert.True(await Scalar("SELECT count(*) FROM jobs WHERE type='FlakyJob' AND attempts > 1") > 0);

        // poison: parked as Dead after exactly max_attempts, with the error stored
        Assert.Equal(5, await Scalar(
            "SELECT count(*) FROM jobs WHERE type='PoisonJob' AND status=4 AND attempts=3 AND last_error IS NOT NULL"));

        // the important one: no job executed its side effect twice
        Assert.Equal(0, await Scalar(
            "SELECT count(*) FROM (SELECT job_id FROM job_effects GROUP BY job_id HAVING count(*) > 1) x"));
    }

    private async Task EnqueueMany(string type, int count, int maxAttempts)
    {
        for (var i = 0; i < count; i++)
            await _repo.EnqueueAsync(new Job
            {
                Id = Guid.NewGuid(),
                Type = type,
                MaxAttempts = maxAttempts,
                RunAt = DateTime.UtcNow.AddSeconds(-1)
            });
    }

    private async Task WaitUntilIdleAsync(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await Scalar("SELECT count(*) FROM jobs WHERE status IN (0,1)") == 0) return;
            await Task.Delay(200);
        }
        throw new TimeoutException("Jobs did not finish in time");
    }

    private async Task<long> Scalar(string sql)
    {
        await using var cmd = _db.CreateCommand(sql);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}