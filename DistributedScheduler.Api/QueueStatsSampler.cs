using System.Diagnostics.Metrics;
using DistributedScheduler.Domain;

public sealed class QueueStatsSampler : BackgroundService
{
    private static readonly JobStatus[] Statuses = Enum.GetValues<JobStatus>();
    private readonly IJobRepository _repo;
    private readonly ILogger<QueueStatsSampler> _log;
    private volatile IReadOnlyDictionary<JobStatus, long> _counts = new Dictionary<JobStatus, long>();

    public QueueStatsSampler(IJobRepository repo, ILogger<QueueStatsSampler> log)
    {
        _repo = repo;
        _log = log;
        Telemetry.Meter.CreateObservableGauge<long>("scheduler.jobs.count", Observe, "{job}", "Jobs currently in each status");
    }

    private IEnumerable<Measurement<long>> Observe() =>
        Statuses.Select(s => new Measurement<long>(
            _counts.GetValueOrDefault(s), new KeyValuePair<string, object?>("status", s.ToString())));

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        do
        {
            try { _counts = await _repo.CountByStatusAsync(ct); }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _log.LogWarning(ex, "Could not sample job counts");
            }
        }
        while (await WaitAsync(timer, ct));
    }

    private static async Task<bool> WaitAsync(PeriodicTimer timer, CancellationToken ct)
    {
        try { return await timer.WaitForNextTickAsync(ct); }
        catch (OperationCanceledException) { return false; }
    }
}