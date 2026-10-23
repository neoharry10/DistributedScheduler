using DistributedScheduler.Domain;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DistributedScheduler.Worker;

public class LeaseReaper(IJobRepository repo, IOptions<WorkerOptions> options, ILogger<LeaseReaper> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(options.Value.ReaperInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                try
                {
                    var n = await repo.ReapExpiredAsync(ct);
                    if (n > 0) {
                        Telemetry.Reaped.Add(n);
                        log.LogWarning("Reaped {Count} job(s) with expired leases", n);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    log.LogError(ex, "Reaper failed, will retry next tick");
                }
            }
        }
        catch (OperationCanceledException) { }
    }
}