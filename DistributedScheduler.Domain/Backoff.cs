namespace DistributedScheduler.Domain;

public static class Backoff
{
    /// attempt is 1-based (the attempt that just failed). jitter is 0..1.
    public static TimeSpan Delay(int attempt, TimeSpan baseDelay, TimeSpan cap, double jitter)
    {
        var exp = baseDelay.TotalSeconds * Math.Pow(2, Math.Max(attempt - 1, 0));
        var capped = Math.Min(exp, cap.TotalSeconds);
        return TimeSpan.FromSeconds(capped * (0.5 + 0.5 * jitter));   // 50-100% of the delay
    }
}