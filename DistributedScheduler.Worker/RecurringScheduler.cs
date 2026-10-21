using DistributedScheduler.Domain;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DistributedScheduler.Worker;

public class RecurringScheduler(IJobRepository repo, IOptions<WorkerOptions> options, ILogger<RecurringScheduler> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(options.Value.SchedulerInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try
                {
                    var n = await repo.EnqueueDueRecurringAsync(ct: ct);
                    if (n > 0) log.LogInformation("Enqueued {Count} recurring job(s)", n);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    log.LogError(ex, "Recurring scheduler failed, will retry next tick");
                }
            }
        }
        catch (OperationCanceledException) { }
    }
}