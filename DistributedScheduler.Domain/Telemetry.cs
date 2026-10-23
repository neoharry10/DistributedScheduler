using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace DistributedScheduler.Domain;

public static class Telemetry
{
    public const string Name = "DistributedScheduler";

    // Seconds-scale buckets. The SDK's defaults are meant for milliseconds and would put every job in the first one.
    private static readonly double[] Buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300];

    public static readonly ActivitySource Source = new(Name);
    public static readonly Meter Meter = new(Name);

    public static readonly Counter<long> Enqueued =
        Meter.CreateCounter<long>("scheduler.jobs.enqueued", "{job}", "Jobs accepted into the queue");

    public static readonly Counter<long> Finished =
        Meter.CreateCounter<long>("scheduler.jobs.finished", "{job}",
            "Job attempts that ended: succeeded, retry, dead, released, lease_lost");

    public static readonly Counter<long> Reaped =
        Meter.CreateCounter<long>("scheduler.jobs.reaped", "{job}", "Jobs rescued after their lease expired");

    public static readonly Histogram<double> Duration =
        Meter.CreateHistogram<double>("scheduler.job.duration", "s", "Time the handler ran",
            advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = Buckets });

    public static readonly Histogram<double> QueueLatency =
        Meter.CreateHistogram<double>("scheduler.job.queue_latency", "s",
            "Time between a job becoming due and its handler starting",
            advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = Buckets });
}