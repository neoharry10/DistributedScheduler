using DistributedScheduler.Domain;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DistributedScheduler.Worker;

public class RetentionService(IJobRepository repo, IOptions<WorkerOptions> options, ILogger<RetentionService> log) : BackgroundService
{
    private const int Batch = 1000;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var o = options.Value;
        using var timer = new PeriodicTimer(o.RetentionInterval);
        try
        {
            do
            {
                try
                {
                    int n, total = 0;
                    do
                    {
                        n = await repo.DeleteFinishedAsync(o.SucceededRetention, o.DeadRetention, Batch, ct);
                        total += n;
                    }
                    while (n == Batch);   // a full batch means there may be more

                    if (total > 0) log.LogInformation("Retention deleted {Count} old finished job(s)", total);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    log.LogError(ex, "Retention failed, will retry next interval");
                }
            }
            while (await timer.WaitForNextTickAsync(ct));
        }
        catch (OperationCanceledException) { }
    }
}