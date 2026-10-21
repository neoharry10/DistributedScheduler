using System.Collections.Concurrent;
using DistributedScheduler.Data;
using DistributedScheduler.Domain;
using Npgsql;
using Testcontainers.PostgreSql;

namespace DistributedScheduler.Tests;

public class ClaimTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _pg = new PostgreSqlBuilder("postgres:16").Build();
    private JobRepository _repo = default!;

    public async Task InitializeAsync()
    {
        await _pg.StartAsync();
        Migrator.Run(_pg.GetConnectionString());
        _repo = new JobRepository(NpgsqlDataSource.Create(_pg.GetConnectionString()));
    }

    public async Task DisposeAsync() => await _pg.DisposeAsync();

    [Fact]
    public async Task No_job_is_claimed_twice()
    {
        for (var i = 0; i < 1000; i++)
            await _repo.EnqueueAsync(new Job
            {
                Id = Guid.NewGuid(),
                Type = "SendEmailJob",
                RunAt = DateTime.UtcNow.AddSeconds(-1)
            });

        var claimed = new ConcurrentBag<Guid>();

        var workers = Enumerable.Range(1, 4).Select(w => Task.Run(async () =>
        {
            while (true)
            {
                var batch = await _repo.ClaimAsync($"worker-{w}", "default", 10, TimeSpan.FromSeconds(30));
                if (batch.Count == 0) break;
                foreach (var j in batch) claimed.Add(j.Id);
            }
        }));

        await Task.WhenAll(workers);

        Assert.Equal(1000, claimed.Count);                    // everything got claimed
        Assert.Equal(1000, claimed.Distinct().Count());       // nothing claimed twice
    }
}