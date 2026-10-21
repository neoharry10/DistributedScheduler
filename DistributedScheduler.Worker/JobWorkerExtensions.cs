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

        services.Configure<WorkerOptions>(o => configure?.Invoke(o));
        services.AddHostedService<JobWorker>();
        return services;
    }
}