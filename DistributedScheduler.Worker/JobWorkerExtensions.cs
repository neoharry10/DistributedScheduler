using DistributedScheduler.Data;
using DistributedScheduler.Domain;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace DistributedScheduler.Worker;

public static class JobWorkerExtensions
{
    public static IServiceCollection AddJobWorker(
        this IServiceCollection services, string connectionString, Action<WorkerOptions>? configure = null)
    {
        services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
        services.AddSingleton<IJobRepository, JobRepository>();

        services.AddScoped<IJobHandler, SendEmailJob>();
        services.AddScoped<IJobHandler, FlakyJob>();
        services.AddScoped<IJobHandler, PoisonJob>();
        services.AddScoped<IJobHandler, SlowJob>();
        services.AddScoped<IJobHandler, TickJob>();
        services.AddScoped<IJobHandler, GenerateInvoiceJob>();
        services.AddScoped<IJobHandler, ProbeJob>();
        services.AddScoped<IJobHandler, NoopJob>();

        services.AddOptions<WorkerOptions>()
            .Configure(o => configure?.Invoke(o))
            .Validate(o => o.HeartbeatInterval * 3 <= o.Lease, "HeartbeatInterval must be at most Lease / 3")
            .Validate(o => o.EffectiveQueues().All(kv => kv.Value.Concurrency >= 1 && kv.Value.BatchSize >= 1),
                "Every queue needs Concurrency >= 1 and BatchSize >= 1")
            .Validate(o => o.RateLimits.All(kv => kv.Value > 0), "Rate limits must be greater than 0")
            .ValidateOnStart();

        services.AddHostedService<JobWorker>();
        services.AddHostedService<LeaseReaper>();
        services.AddHostedService<RecurringScheduler>();
        return services;
    }
}