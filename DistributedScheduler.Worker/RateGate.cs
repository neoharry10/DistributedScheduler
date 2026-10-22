using System.Diagnostics;

namespace DistributedScheduler.Worker;

/// Paces starts evenly: at most `perSecond` callers get through per second, spread out (no bursts).
public sealed class RateGate
{
    private readonly long _interval;      // stopwatch ticks between two permits
    private readonly object _lock = new();
    private long _next;                   // earliest timestamp the next caller may start at

    public RateGate(double perSecond)
    {
        if (perSecond <= 0) throw new ArgumentOutOfRangeException(nameof(perSecond));
        _interval = (long)(Stopwatch.Frequency / perSecond);
    }

    public Task WaitAsync(CancellationToken ct)
    {
        long wait;
        lock (_lock)
        {
            var now = Stopwatch.GetTimestamp();
            var start = Math.Max(now, _next);
            _next = start + _interval;     // reserve our slot, the next caller queues behind it
            wait = start - now;
        }
        return wait <= 0 ? Task.CompletedTask : Task.Delay(Stopwatch.GetElapsedTime(0, wait), ct);
    }
}