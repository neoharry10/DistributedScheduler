using Microsoft.Extensions.Hosting;
using Npgsql;

namespace DistributedScheduler.Web.Services;

/// Listens for NOTIFY on 'jobs_changed' and tells the UI, at most ~4 times a second.
public sealed class JobChangeFeed(NpgsqlDataSource db, ILogger<JobChangeFeed> log) : BackgroundService
{
    private int _pending;

    public event Action? Changed;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _ = Task.Run(() => TickAsync(ct), CancellationToken.None);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // A dedicated connection: it sits in WaitAsync forever, so it can't be shared.
                await using var conn = await db.OpenConnectionAsync(ct);
                conn.Notification += (_, _) => Poke();

                await using (var cmd = new NpgsqlCommand("LISTEN jobs_changed", conn))
                    await cmd.ExecuteNonQueryAsync(ct);

                Poke();   // we may have missed changes while disconnected
                while (!ct.IsCancellationRequested) await conn.WaitAsync(ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Job change listener lost its connection, retrying in 2s");
                try { await Task.Delay(TimeSpan.FromSeconds(2), ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    // Slides the chart's time axis even when nothing is happening.
    private async Task TickAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));
        try { while (await timer.WaitForNextTickAsync(ct)) Poke(); }
        catch (OperationCanceledException) { }
    }

    private void Poke()
    {
        if (Interlocked.Exchange(ref _pending, 1) == 1) return;   // a refresh is already scheduled
        _ = Task.Run(async () =>
        {
            await Task.Delay(250);
            Volatile.Write(ref _pending, 0);   // reset BEFORE invoking, so changes during the refresh schedule another
            try { Changed?.Invoke(); }
            catch (Exception ex) { log.LogError(ex, "A dashboard subscriber threw"); }
        });
    }
}